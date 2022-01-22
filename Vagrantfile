# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.box = "bento/ubuntu-21.04"

  config.vm.synced_folder ".", "/home/vagrant/gestalt"

  config.vm.provider :virtualbox do |vb|
    vb.cpus = 4
    vb.memory = 4096
  end

  # Configure RDMA (Soft-RoCE)
  config.vm.provision :shell, inline: <<~SHELL
    # Install RDMA packages
    apt update
    apt install -y ibverbs-utils rdma-core rdmacm-utils
    # Configure Soft-RoCE startup
    cat << EOF > /etc/systemd/system/soft-roce-startup.service
    [Unit]
    Description=Configure Soft RoCE on boot
    [Service]
    ExecStart=/usr/local/bin/soft-roce-startup.sh
    [Install]
    WantedBy=multi-user.target
    EOF
    cat << EOF > /usr/local/bin/soft-roce-startup.sh
    #!/bin/bash
    modprobe rdma_rxe
    rdma link add rxe_0 type rxe netdev eth0
    EOF
    chmod a+x /usr/local/bin/soft-roce-startup.sh
    systemctl enable soft-roce-startup
    systemctl start soft-roce-startup
    sleep 3
    # Validation
    echo "Listing RDMA devices..."
    rdma link
    ibv_devinfo
  SHELL

  # Configure PMem (memmap)
  # NOTE: you will need to manually reboot the vm after first created,
  # the `reboot` keyword just don't work well
  config.vm.provision :shell, inline: <<~SHELL
    # Install PMem software
    apt install -y ndctl ipmctl
    # Create emulated PMem
    sed -i -e '/^GRUB_CMDLINE_LINUX=/ s/ "/ memmap=512M!1G"/' /etc/default/grub
    update-grub2
    echo "Manually reboot vm to apply memmap changes!"
  SHELL

  config.vm.define "gestalt-playground" do |test|
    test.vm.network :forwarded_port, guest: 22, host: 53276
  end
end

