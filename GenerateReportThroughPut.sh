echo $2" Operations/sec (more is better)(AVG,MAX,MIN):"
cat $1 |  awk '{if(found) print} /(less is better)/{found=1}' |cut -d',' -f2 |awk '{if(min==""){min=max=$1}; if($1>max) {max=$1}; if($1<min) {min=$1}; total+=$1; count+=1} END {print total/count, max, min}'
echo "------------------------------------------------------------------------------------------"

