. buildscripts/ec2_variables.sh
ec2-run-instances ami-57360a20 -t t1.micro --region eu-west-1 -k $key -g sg-8ec476eb  
# Wait for instance to be running.
tries=20
while ! ec2-describe-instances|grep -q running >&2; do
  let tries=tries-1
  if [ $tries -eq 0 ] ; then
    break
  fi
  sleep 5
done
sleep 5
IP=$(ec2-describe-instances|awk '{ if($1=="PRIVATEIPADDRESS") { print $4 } }')
echo IP:$IP >&2
echo $IP

