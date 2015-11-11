. buildscripts/ec2_variables.sh
echo Starting EC2 instance. >&2
ec2-run-instances ami-57360a20 -t t1.micro --region eu-west-1 -k $key -g sg-8ec476eb >&2
# Wait for instance to be accessible.
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
echo EC2 instance details: >&2
ec2-describe-instances >&2
sleep 30
IP=$(ec2-describe-instances|grep running|awk '{ if($1=="INSTANCE") { print $4; exit 0 } }')
echo IP:$IP >&2
echo $IP

