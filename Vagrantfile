# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure(2) do |config|
  config.vm.box = 'emyl/hortonworks-sandbox-2.1'
  config.vm.box_url = 'https://atlas.hashicorp.com/emyl/boxes/hortonworks-sandbox-2.1'

  # configure VM settings
  config.vm.network :public_network
  config.ssh.forward_agent = true

  config.vm.provider :virtualbox do |vb, override|
    # let's forward the server's port 80 to our 8080
    override.vm.network :forwarded_port, guest: 80, host: 8080

    vb.memory = 2048
    vb.cpus = 2
  end

  config.push.define 'ftp' do |push|
    push.host = '127.0.0.1'
    push.port = 21
    push.username = 'vagrant'
  end
end
