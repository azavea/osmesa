# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.require_version ">= 1.8"

Vagrant.configure(2) do |config|
  config.vm.box = "ubuntu/trusty64"

  config.vm.synced_folder "~/.aws", "/home/vagrant/.aws"
  config.vm.synced_folder "~/.ivy2", "/home/vagrant/.ivy2"
  config.vm.synced_folder "~/.m2", "/home/vagrant/.m2"

  config.vm.provider :virtualbox do |vb|
    vb.memory = 4096
    vb.cpus = 4
  end

  # UIs
  # Accumulo: http://localhost:50095
  # Hadoop DFS: http://localhost:50070
  # Job Tracker: http://localhost:8088
  # Zeppelin: http://localhost:5771
  config.vm.network :forwarded_port, guest: 50095, host: 50095
  config.vm.network :forwarded_port, guest: 50070, host: 8088
  config.vm.network :forwarded_port, guest: 5771, host: 5771

  # HBase
  config.vm.network :forwarded_port, guest: 60000, host: 60000
  config.vm.network :forwarded_port, guest: 60010, host: 60010


  # Change working directory to /vagrant upon session start.
  config.vm.provision "shell", inline: <<SCRIPT
    if ! grep -q "cd /vagrant" "/home/vagrant/.bashrc"; then
      echo "cd /vagrant" >> "/home/vagrant/.bashrc"
    fi
SCRIPT

  config.vm.provision "ansible" do |ansible|
    ansible.playbook = "ansible/osmesa.yml"
    ansible.galaxy_role_file = "ansible/roles.yml"
  end
end
