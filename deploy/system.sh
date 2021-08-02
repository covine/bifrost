#!/bin/bash

{
  echo 'net.core.rmem_max = 16777216'
  echo 'net.core.somaxconn = 65535'
  echo 'net.core.rmem_default = 262144'
  echo 'net.core.wmem_default = 262144'
  echo 'net.core.wmem_max = 16777216'
  echo 'net.core.optmem_max = 16777216'
  echo 'net.core.netdev_max_backlog = 30000'
  echo 'net.ipv4.tcp_max_syn_backlog = 262144'
  echo 'net.ipv4.tcp_max_tw_buckets = 1048576'
  echo 'net.ipv4.tcp_tw_recycle = 0'
  echo 'net.ipv4.ip_local_port_range = 1000 65535'
  echo 'net.ipv4.tcp_rmem = 1024 4096 16777216'
  echo 'net.ipv4.tcp_wmem = 1024 4096 16777216'
  echo 'net.ipv4.tcp_fin_timeout = 30'
  echo 'net.ipv4.tcp_keepalive_time = 600'
  echo 'net.ipv4.tcp_keepalive_intvl = 30'
  echo 'net.ipv4.tcp_keepalive_probes = 10'
  echo 'net.ipv4.conf.default.rp_filter = 0'
  echo 'net.ipv4.conf.all.rp_filter = 0'
  echo 'net.ipv4.tcp_synack_retries = 2'
  echo 'net.ipv4.ip_forward = 1'
  echo 'net.ipv4.ip_nonlocal_bind = 1'
  echo 'net.ipv6.conf.all.disable_ipv6 = 1'
  echo 'net.ipv6.conf.default.disable_ipv6 = 1'
  echo 'net.ipv6.conf.lo.disable_ipv6 = 1'
  echo 'net.nf_conntrack_max = 1000000'
  echo 'net.netfilter.nf_conntrack_max = 1000000'
  echo 'net.netfilter.nf_conntrack_tcp_timeout_time_wait = 30'
  echo 'net.netfilter.nf_conntrack_tcp_timeout_established = 1200'
  echo 'net.bridge.bridge-nf-call-arptables = 1'
  echo 'net.bridge.bridge-nf-call-iptables = 1'
  echo 'net.bridge.bridge-nf-call-ip6tables = 1'
  echo 'fs.nr_open = 5242880'
  echo 'fs.file-max = 5242880'
  echo 'fs.may_detach_mounts = 1'
  echo 'vm.swappiness = 0'
  echo 'vm.panic_on_oom = 0'
  echo 'vm.overcommit_memory = 1'
  echo 'vm.max_map_count = 65536'
} >> /etc/sysctl.conf

sysctl --system

{
  echo 'root soft nofile 2048576'
  echo 'root hard nofile 2048576'
  echo '*    soft nofile 2048576'
  echo '*    hard nofile 2048576'
} >> /etc/security/limits.conf

reboot
