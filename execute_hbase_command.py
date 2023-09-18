import paramiko
host = "sumishra-acc-2.sumishra-acc.root.hwx.site"
def run_hbase_commands(command_name):
  key_pair = "/Users/vjoshi/.ssh/ycloud_keypair.pem"
  keytab = "kinit -kt /cdep/keytabs/hrt_qa.keytab hrt_qa@ROOT.HWX.SITE"
  ssh = paramiko.SSHClient()
  ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  ssh.connect(hostname=host, username="root", key_filename=key_pair)
  #print("--> Connected")

  print("echo \"%s\"", str(command_name) )
  command_file = "echo \"%s\" >> /tmp/a.txt" % command_name
  prefetch_commands = ["rm /tmp/a.txt", command_file, keytab]
  for command in prefetch_commands:
    stdin, stdout, stderr = ssh.exec_command(command)

  stdin, stdout, stderr = ssh.exec_command("hbase shell < /tmp/a.txt")
  print(str(stdout.readlines()))
  ssh.close()
  return stdout.readlines()

if __name__ == '__main__':
    run_hbase_commands(list)