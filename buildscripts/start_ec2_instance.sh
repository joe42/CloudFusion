. buildscripts/ec2_variables.sh
# Wait for instance to be running.
tries=40
while ! ec2-describe-instances|grep -q running >&2; do
  let tries=tries-1
  if [ $tries -eq 0 ] ; then
    echo Waited too long for EC2 instance to start.>&2
    break
  fi
  sleep 5
done
sleep 5
IP=$(ec2-describe-instances|awk '{ if($1=="PRIVATEIPADDRESS") { print $4 } }')
echo IP:$IP >&2
echo $IP

