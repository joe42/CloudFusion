. buildscripts/ec2_variables.sh
echo Starting EC2 instance.
ec2-run-instances ami-57360a20 -t t1.micro --region eu-west-1 -k $key -g sg-8ec476eb  >&2
# Wait for instance to be running.
tries=40
while ! ec2-describe-instances|grep -q running; do
  let tries=tries-1
  if [ $tries -eq 0 ] ; then
    echo Waited too long for EC2 instance to start:>&2
    ec2-describe-instances>&2
    break
  fi
  sleep 5
done
ec2-describe-instances >&2
sleep 10
IP=$(ec2-describe-instances|awk '{ if($1=="PRIVATEIPADDRESS") { print $4 } }')
echo IP:$IP >&2
echo $IP

