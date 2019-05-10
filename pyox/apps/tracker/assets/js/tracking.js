(function() {

function updateTracking(data) {
   $("#tracking tbody").empty();
   if (data.length==0) {
      var d = new Date();
      $("#tracking tbody").append(
         `<tr><td colspan='4'>No jobs are being tracked (${d.toISOString()}).</td></tr>`
      )
   }
   data.sort(function (a,b) {
      if (a["last-checked"] > b["last-checked"]) {
         return -1
      }
      if (a["last-checked"] < b["last-checked"]) {
         return 1
      }
      return 0;
   });
   for (info of data) {
      apps = "";
      logs = "";
      for (id of info['application-ids']) {
         app = info.applications[id];
         status = app===undefined ? 'NOT COPIED' : (app.status=='SUCCEEDED' ? 'COPIED' : app.status);
         apps += `<p class="application-status" data-application="${id}"><a href="/api/job/${info.id}/logs/${id}" target="_blank"><span class="status">${status}</span> : <span class="application">${id}</span></a></p>`
      }
      $("#tracking tbody").append(
         `<tr><td data-id="${info.id}">${info.id} <a href="#" class="copy-logs" title="Copy logs if needed" uk-icon="icon: copy"></a> <a href="#" class="force-copy-logs" title="Force a log copy" uk-icon="icon: bolt"></a></td><td>${info.status}</td><td class="uk-table-expand">${apps}</td></tr>`
      )
   }

   $("#tracking tr .copy-logs").on("click",function() {
      console.log(`Requesting log copy for ${this.parentNode.dataset.id} ...`);
      copyJobLogs(this.parentNode.dataset.id,this,false)
      return false;
   });

   $("#tracking tr .force-copy-logs").on("click",function() {
      var link = this;
      UIkit.modal.confirm('Copying logs may overwrite existing logs. Are you sure you want to proceed?').then(function() {
         console.log(`Requesting log copy for ${link.parentNode.dataset.id} ...`);
         copyJobLogs(link.parentNode.dataset.id,link,true)
      });
      return false;
   });

}

function appendTracking(data) {
   for (info of data) {
      apps = "";
      for (id of info['applications-ids']) {
         apps += `<p class="application-status" data-application="${id}"><a href="/api/job/${info.id}/logs/${id}" target="_blank"><span class="status">NOT COPIED</span> : <span class="application">${id}</span></a></p>`
      }
      $("#tracking tbody").append(
         `<tr><td data-id="${info.id}">${info.id} <a href="#" class="copy-logs" title="Copy logs if needed" uk-icon="icon: copy"></a> <a href="#" class="force-copy-logs" title="Force a log copy" uk-icon="icon: bolt"></a></td><td>${info.status}</td><td class="uk-table-expand">${apps}</td></tr>`
      )
      $(`#tracking tbody td[data-id="${info.id}"] .copy-logs`).on("click",function() {
         console.log(`Requesting log copy for ${this.parentNode.dataset.id} ...`);
         copyJobLogs(this.parentNode.dataset.id,this,false);
         return false;
      });
      $(`#tracking tbody td[data-id="${info.id}"] .force-copy-logs`).on("click",function() {
         var link = this;
         UIkit.modal.confirm('Copying logs may overwrite existing logs. Are you sure you want to proceed?').then(function() {
            console.log(`Requesting log copy for ${link.parentNode.dataset.id} ...`);
            copyJobLogs(link.parentNode.dataset.id,link,true);
         });
         return false;
      });

   }
}

function refreshTracking() {
   $("#tracking h2").append("<div uk-spinner class='loading'></div>");
   fetch('/api/jobs/tracking?refresh=true',{ credentials: 'include'}).then(function(response){
      $("#tracking h2 .loading").remove();
      if (response.status !== 200) {
         console.log("Error getting job tracking, status "+response.status);
         return;
      }
      console.log("Response, refreshing tracking display.")
      response.json().then(function(data) {
         updateTracking(data);
      });
   }).catch(function(err) {
      console.log("Error getting tracking information:-S",err);
   });
}

function trackJob(id) {
   $("#track-job-working").append(" <span uk-spinner></span>");
   $("#track-job button").attr("disabled","disabled");
   fetch("/api/task/track/", {
      credentials: 'include',
      method: 'post',
      headers: {
         'Content-Type':'text/plain'
      },
      body: id
   }).then(function(response) {
      $("#track-job button").removeAttr("disabled");
      $("#track-job-working").empty();
      if (response.status !== 200) {
         console.log("Error getting job tracking, status "+response.status);
         return;
      }
      response.json().then(function(data) {
         appendTracking(data);
      });
   }).catch(function(error) {
      $("#track-job button").removeAttr("disabled");
      $("#track-job-working").empty();
      console.log(`Error posting tracking job ${id}:-S`,error);
   });
}

function copyJobLogs(id,parent,force) {
   $(parent).find("svg").attr("hidden","hidden");
   $(parent).append("<span uk-spinner></span>");
   fetch(`/api/task/copy-logs/?refresh=${force}`, {
      credentials: 'include',
      method: 'post',
      headers: {
         'Content-Type':'text/plain'
      },
      body: id
   }).then(function(response) {
      $(parent).find("span[uk-spinner]").remove();
      $(parent).find("svg").removeAttr("hidden");
      UIkit.notification(`Requested logs to be copied for job ${id}`);
      response.json().then(function(data) {
         let row = parent.parentNode.parentNode;
         for (let app of data.jobs) {
            let status = app.status=='SUCCEEDED' ? 'COPIED' : app.status;
            $(row).find(`p[data-application="${app.application}"] .status`).text(status);
         }
      });
   }).catch(function(error) {
      $(parent).find("span[uk-spinner]").remove();
      $(parent).find("svg").removeAttr("hidden");
      UIkit.notification(`Unable to copy logs for job ${id}`);
      console.log(`Error posting tracking job ${id}:-S`,error);
   });
}
$(document).ready(function() {
   setTimeout(refreshTracking,10);
   $("#tracking .refresh").on("click",function() {
      setTimeout(function() { refreshTracking(); });
      return false;
   });
   $("#track-job button").on("click",function() {
      let id = $("#track-job input[name=id]").val();
      console.log(`Tracking job ${id} ...`);
      trackJob(id);
      return false;
   });
});

})();
