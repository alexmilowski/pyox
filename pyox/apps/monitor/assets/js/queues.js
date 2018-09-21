
function showQueue(level,parent,queueInfo) {
   let queueContainer = $(`<section class="queue level-${level}"><h3>${queueInfo.queueName}</h3><section class="children"></section></section>`)
   $(parent).append(queueContainer);
   let childrenSection = $(queueContainer).find(".children")[0];
   if (queueInfo.queues) {
      queueByName = {}
      for (let child of queueInfo.queues.queue) {
         queueByName[child.queueName] = child;
      }
      for (let queueName of Object.keys(queueByName).sort(function (a,b) { return a.toLowerCase().localeCompare(b.toLowerCase())})) {
         showQueue(level+1,childrenSection,queueByName[queueName]);
      }
   }
}

function leafQueueData(data,queueInfo) {
   if (queueInfo.queues) {
      for (let child of queueInfo.queues.queue) {
         leafQueueData(data,child);
      }
   }
   if (queueInfo.type!="capacitySchedulerLeafQueueInfo") {
      return;
   }
   let remaining = Math.round(queueInfo.absoluteCapacity - queueInfo.absoluteUsedCapacity);
   let used = Math.round(queueInfo.absoluteUsedCapacity);
   let over = 0;
   let max = Math.round(queueInfo.absoluteCapacity - queueInfo.absoluteMaxCapacity);
   if (remaining<0) {
      used = queueInfo.absoluteCapacity
      over = remaining;
      remaining = 0
      max = Math.round(max - over);
   } else {
      remaining = remaining;
   }
   data.push({name:queueInfo.queueName,remaining:remaining,used:used,over:over,max:max,users:queueInfo.users});
   console.log(queueInfo);
}

function queueView(id,queueInfo) {

   let data = [];
   leafQueueData(data,queueInfo);
   let columns = [
      ["names"],
      ["remaining"],
      ["used"],
      ["over"],
      ["max"]
   ];
   $("#job-list tbody").empty();
   for (let info of data.sort(function (a,b) { return a.name.toLowerCase().localeCompare(b.name.toLowerCase())})) {
      columns[0].push(info.name);
      columns[1].push(info.remaining);
      columns[2].push(info.used);
      columns[3].push(info.over);
      columns[4].push(info.max);
      if (info.users) {
         for (let user of info.users.user) {
            $("#job-list tbody").append(
               `<tr>
                  <td>${info.name}</td>
                  <td>${user.username}</td>
                  <td>${user.numActiveApplications}</td>
                  <td>${user.numPendingApplications}</td>
                  <td>${user.resourcesUsed.memory}</td>
                  <td>${user.resourcesUsed.vCores}</td>
                  <td>${user.userResourceLimit.memory}/${user.userResourceLimit.vCores}</td>
               </tr>`
            );
            console.log(user);
         }
      }
   }
   var chart = c3.generate({
      bindto: id,
      data: {
         x : "names",
         type:"bar",
         columns: columns,
         groups: [["remaining","used","over","max"]]
      },
      axis: {
         rotated: true,
         x: {
            type: "category"
         }
      },
      grid: {
        y: {
            lines: [{value:0}]
        }
     },
     color: {
        pattern: ["#dac9e2","#74dd1f","#af1616","#dbc5c5"]
     }
   });
}

var queueContext = {};

function refreshQueueView(callback) {
   console.log("Requesting queue refresh...");
   fetch('/api/cluster/scheduler',{ credentials: 'include'}).then(function(response){
      if (response.status !== 200) {
         console.log("Error getting queue, status "+response.status);
         return;
      }
      console.log("Response, refreshing queue display.")
      response.json().then(function(data) {
         $("#queue-view").empty();
         queueView("#queue-view",data);
         if (callback) {
            callback();
         }
      });
   }).catch(function(err) {
      console.log("Error getting queue information:-S",err);
   });
}


function queueRefresh() {
   let delay =parseInt($("#refresh-rate")[0].value)*1000;
   console.log("Delay: "+delay);
   queueContext.timer = setTimeout(function() {
      queueContext.scrollTop = $(window).scrollTop();
      queueContext.preserveScroll = true;
      if (queueContext.refreshing) {
         refreshQueueView(function() {
            if (queueContext.preserveScroll) {
               $(window).scrollTop(queueContext.scrollTop);
            }
            if (queueContext.refreshing) {
               queueRefresh();
            }
         });
      }
   },delay);
   queueContext.refreshing = true;
}

function cancelRefresh() {
   queueContext.refreshing = false;
   clearTimeout(queueContext.timer);
}


$(document).ready(function() {
   $(window).on("scroll",function() {
      queueContext.scrollTop = $(window).scrollTop();
   })
   $("#queues .refresh").on("click",function() {
      setTimeout(function() { refreshQueueView(); });
      return false;
   });
   $("#refresh-rate").on("change",function() {
      $("#refresh-value").text($("#refresh-rate")[0].value+"s");
   })
   $("#queues .play-pause").on("click",function() {
      let play = $(this).find(".fa-play").length>0;
      if (play) {
         $(this).find(".fa").removeClass("fa-play");
         $(this).find(".fa").addClass("fa-pause");
         $(this).attr("title","Pause Refresh");
         queueRefresh();
      } else {
         $(this).find(".fa").removeClass("fa-pause");
         $(this).find(".fa").addClass("fa-play");
         $(this).attr("title","Automatically Refresh");
         cancelRefresh();
      }
      return false;
   });
})
