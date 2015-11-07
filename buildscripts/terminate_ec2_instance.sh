. buildscripts/ec2_variables.sh
ID=$(ec2-describe-instances|awk '{ if($1=="INSTANCE") { print $2 } }')
ec2-terminate-instances $ID
