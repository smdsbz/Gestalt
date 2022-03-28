Gestalt
=======



How to run
----------

### Single client

1.  Install dependencies

    ```console
    # ./install-deps-ub21.sh
    ```

    > Should work for Ubuntu 21.04 and above, or you will need to manually
    > install Boost 1.74 or above.

2.  Build with CMake
3.  Check config file at `etc/gestalt/gestalt.conf`
4.  On monitor machine

    ```console
    # build/bin/gestalt_monitor
    ```

5.  On each server machine, aka memory node

    ```console
    # build/bin/gestalt_server --addr <RNIC_IP> --dax-dev /dev/daxX.X
    ```

    And wait for them to report they're ready

    ```text
    [info]    Server up and running!
    ```

    Depending on your PMem size and CPU processing power, this may take from
    seconds to a minute.

    > PMem namespace should be in DEVDAX mode.

6.  On client machine

    ```console
    # build/bin/benchmark_latency   # bw and iops included, didn't bother renaming it
    ```

Sometimes the benchmark client may fail and report `-EBADR`, due to mismatch in
RDMA work request commit / consume speed when thread count is high, which highly
depends on hardware processing power of your specific CPU / RNIC combination,
try restart the cluster and re-run.


### Multiple clients

1.  Set up NTP, preferably over IB interface, so that clients are peers with each
    other

    The distributed benchmark requires sync-ed time for workloads to start
    simultaneously, at least roughly.

2.  Set up NFS among client machines

    The distributed benchmark client on each server should be run from the same
    directory, which will be used to publish workload trace among clients.

3.  Start benchmark clients

    ```console
    # build/bin/benchmark_dist_follower --id <CLIENT_ID>
    ```

    Supply unique client ID to each benchmark clients, starting from 2.

4.  Set off

    ```console
    # build/bin/benchmark_dist_starter
    ```

    Any node will do.
