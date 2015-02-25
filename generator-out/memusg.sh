#!/bin/bash
peak=0
sleep 0.1
while true; do
  sleep 0.05
  pid=$(ps aux | grep /data/lab/dashti/.*Q.*.out | awk '{if($11 ~ "^/data") print $2}')
  if [[ "$pid" =~ ^\ +$ || "$pid" =~ ^$ ]]; then
  	break
  else
    sample="$(ps -o rss= $pid 2> /dev/null)" || break
    re='^[0-9]+$'
    if [[ "$sample" =~ $re ]]; then
      if [[ "$sample" -gt "$peak" ]]; then peak="$sample"; fi;
    fi
  fi
done
echo "Peak Memory: $peak"
