/**
 *  @file
 *
 *  A _headless_ static hash table, which is essentially a big vector wrapped with
 *  fancy helper functions. The hash table is headless, meaning that the hash
 *  table instance itself does NOT know what's in itself.
 *
 *  PMem is essentially a pre-allocated, hardware-backed giant array, for it
 *  natively provides byte-addressability (at least sort of), and we'd like our
 *  clients talk directly to this passive RDMA-able storage, instead of having to
 *  query some space allocation (Octopus [ATC'17]) or multi-version metadata
 *  (Orion [FAST'19], Clover [ATC'20]). And yes, I mean __no metadata at all__ ,
 *  not even client-resident metadata cache, since anything that has to deal with
 *  consistency is not that cloud-native IMO :)
 */

#pragma once

#include <cstdint>
#include <limits>
#include <exception>
#include <concepts>
#include <compare>
#include <boost/core/noncopyable.hpp>


namespace gestalt {

using namespace std;

namespace{
template <typename E>
concept __HeadlessHashTable_Entry =
    /* members */
    requires (E e, std::string k) {
        typename E::key_type; { e.key() } noexcept; { e.key().c_str() } noexcept;
        { e.key() == e.key() } noexcept;
        typename E::value_type; { e.value() } noexcept;
    } &&
    /* constructors */
    std::is_standard_layout_v<E> &&
    /* NOTE: default-constructed entry should be invalid */
    std::is_nothrow_default_constructible_v<E> &&
    requires (std::string k, E e) {
        { E(k, e.value()) }; { E(k.c_str(), e.value()) };
    } &&
    /* NOTE: please do remember to overwrite copy ctor and assign op for the
        persistent version */
    /* (user) data */
    requires (E e, std::string k) {
        { E::key_type::hash(k) } noexcept -> std::unsigned_integral;
        { e.key().hash() } noexcept -> std::unsigned_integral;
    } &&
    /* metadata */
    requires (E e) {
        { e.invalidate() } noexcept -> std::same_as<void>;
        { e.is_invalid() } noexcept -> std::same_as<bool>;
        { e.is_valid() } noexcept -> std::same_as<bool>;
    } &&
    /* CMBK: more interfaces */true;
}

/**
 *  Runtime helper class of the headless hash table.
 *
 *  Not thread-safe, as its ignorant nature implies.
 *
 *  @tparam E Entry type, has to be POD and default-constructible, and the
 *      default-constructed value must indicate an empty hash slot. You may also
 *      want to overwrite default copy / assign constructors of `E` as the default
 *      ones clearly does not target PMem.
 *  @sa __HeadlessHashTable_Entry
 */
template <__HeadlessHashTable_Entry E>
class HeadlessHashTable : private boost::noncopyable {
public:
    using entry_type = E;
    /* idk y but the `typename` is mandatory here, despite it is already
        specified in concept */
    using key_type = typename entry_type::key_type;
    using value_type = typename entry_type::value_type;

private:
    using Self = HeadlessHashTable<E>;

private:
    entry_type *_d; ///< pointer to RDMA-able PMem storage, d for data
    const size_t _capacity; ///< length of buffer `d` in objects
    const size_t _max_search;   ///< maximum linear search length

public:
    /**
     *  @param d Data storage buffer
     *  @param capacity Length of buffer `d` in object count
     *  @param max_search_length Maximum linear search length, w/o the original
     *      one
     *  @throw std:invalid_argument
     */
    HeadlessHashTable(entry_type *d, size_t capacity, size_t max_search_length = 5)
        : _d(d), _capacity(capacity), _max_search(max_search_length)
    {
        if (_capacity >= numeric_limits<decltype(_capacity)>::max() - 1)
            throw std::invalid_argument("capacity too big");
    };

    /* iterator */
private:
    struct iterator {
        Self *_ht;
        size_t _i;  ///< current index
        explicit iterator(Self *ht) noexcept : _ht(ht), _i(ht->max_size()) {}
        /**
         *  @throw std::out_of_range
         *  @throw std::runtime_error pointing to invalid entry
         */
        entry_type &operator*()
        {
            if (_i >= _ht->max_size())
                throw std::out_of_range("iterating out of table");
            auto &e = _ht->_d[_i];
            if (e.is_invalid())
                throw std::runtime_error("iterator pointing to invalid entry");
            return e;
        }
        void operator++()
        {
            if (_i >= _ht->max_size())
                throw std::out_of_range("iterating out of table");
            for (++_i; _i < _ht->max_size() && _ht->_d[_i].is_invalid(); ++_i) ;
        }
        inline bool operator==(const iterator &that) const
        {
            if (this->_ht == nullptr || that._ht == nullptr
                    || this->_ht != that._ht)
                throw std::runtime_error("can't compare");
            return this->_i == that._i;
        }
    };  /* struct iterator */
public:
    iterator begin() noexcept
    {
        auto it = iterator(this);
        it._i = 0;
        if (_d[0].is_valid())
            return it;
        ++it;
        return it;
    }
    iterator end() noexcept
    {
        return iterator(this);
    }

public:
    /* capacity */

    /**
     *  @return underlying size
     */
    inline size_t max_size() const noexcept
    {
        return _capacity;
    }

    /* modifiers */

    void clear() noexcept
    {
        /* PERF: currently done via copying one-by-one, and implementation
            may override copy / assign behavior. */
        for (size_t i = 0; i < _capacity; i++)
            _d[i].invalidate();
    }

    /**
     *  Explicitly insert entry
     *  @param e
     *  @throw std::overflow_error key already exist
     *  @throw std::bad_alloc no space left
     */
    void insert(const entry_type &e)
    {
        auto &cell = (*this)[e.key()];
        if (cell.is_valid())
            throw std::overflow_error("key already exist");
        cell = e;
    }

    /* lookup */

    /**
     *  Safe indexing
     *  @throw std::out_of_range no such element
     */
    entry_type &at(const key_type &k)
    {
        auto e = (*this)[k];
        if (e.is_invalid())
            throw std::out_of_range("no such element");
        return e;
    }

    /**
     *  Index or insert
     *  @param k key
     *  @return on found, returns the cell; otherwise an empty, i.e. invalid
     *      cell is returned
     *  @throw std::bad_alloc on key does not exist and no available cell for
     *      insert to take place
     */
    entry_type &operator[](const key_type &k)
    {
        const size_t b = k.hash();
        entry_type *first_avail = nullptr;
        /* linear search */
        for (size_t off = 0; off < _max_search; ++off) {
            auto &e = _d[(b + off) % max_size()];
            if (first_avail == nullptr && e.is_invalid())
                first_avail = &e;
            if (e.is_valid() && e.key() == k)
                return e;
        }
        if (first_avail == nullptr)
            throw std::bad_alloc();
        return *first_avail;
    }

    inline bool contains(const key_type &k) const noexcept
    {
        return const_cast<Self&>(*this)[k].is_valid();
    }

    /* hash policy */

    /**
     *  Compute load factor
     *
     *  PERF: O(n)
     */
    float load_factor() const noexcept
    {
        size_t valid_cnt = 0;
        for (const auto &e : *this) {
            if (e.is_valid())
                valid_cnt++;
        }
        return static_cast<float>(valid_cnt) / _capacity;
    }

    /**
     *  [Debug] Linear search distance of this entry, as for now
     *  @throw std::out_of_range key does not exist
     */
    long access_distance(const key_type &k) const
    {
        const size_t b = k.hash();
        for (size_t off = 0; off < _max_search; ++off) {
            const auto &e = _d[(b + off) % max_size()];
            if (e.is_valid() && e.key() == k)
                return off;
        }
        throw std::out_of_range("no such element");
    }
};  /* class HeadlessHashTable */

}   /* namespace gestalt */
