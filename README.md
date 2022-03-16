Gestalt
=======



How to run
----------

1.  Install dependencies

    ```console
    # ./install-deps-ub21.sh
    ```

    > Should work for Ubuntu 21.04 and above, or you will need to manually
    > install Boost 1.74 or above.

2.  Check config file at `etc/gestalt/gestalt.conf`
3.  On monitor machine

    ```console
    # build/bin/gestalt_monitor
    ```

4.  On each server machine, aka memory node

    ```console
    # build/bin/gestalt_server --addr <RNIC_IP> --dax-dev /dev/daxX.X
    ```

    > PMem namespace should be in DEVDAX mode.

5.  On client machine

    ```console
    # build/bin/benchmark_latency   # bw and iops included, didn't bother renaming it
    ```
