echo $2" CPU-USER(AVG,MAX,MIN):"

cat $1 | awk '{if(found) print} /\"CPU Wait, %\"/{found=1}' |cut -d',' -f14 |awk '{if(min==""){min=max=$1}; if($1>max) {max=$1}; if($1<min) {min=$1}; total+=$1; count+=1} END {print total/count, max, min}'

echo $2" CPU Idle (AVG,MAX,MIN):"

cat $1 |  awk '{if(found) print} /\"CPU Wait, %\"/{found=1}' |cut -d',' -f16 |awk '{if(min==""){min=max=$1}; if($1>max) {max=$1}; if($1<min) {min=$1}; total+=$1; count+=1} END {print total/count, max, min}'

echo $2" Processes Waiting For Run Time(AVG,MAX,MIN):"
cat $1 |  awk '{if(found) print} /\"CPU Wait, %\"/{found=1}' |cut -d',' -f2 |awk '{if(min==""){min=max=$1}; if($1>max) {max=$1}; if($1<min) {min=$1}; total+=$1; count+=1} END {print total/count, max,  min}'

echo @$2" Completion Time:"
cat $1 |  awk '{if(found) print} /\"CPU Wait, %\"/{found=1}' |cut -d',' -f2 |awk '{if(min==""){min=max=$1}; if($1>max) {max=$1}; if($1<min) {min=$1}; total+=$1; count+=1} END {print count}'

echo "---------------------------------------------------------------------------------------"
