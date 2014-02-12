function ActivitiesController($scope, $http) {

  $scope.activities = [];

  $scope.ServiceInfo = $.map(tapiriik.ServiceInfo, function(e) {return e;});

  $scope.DisplayNameByService = function(svcId){ return tapiriik.ServiceInfo[svcId].DisplayName; };

  $scope.ExceptionExplanation = function(presc){
    var type = presc.Exception.Type;
    var explanations = {
      "auth": "The credentials you entered for %(service) are no longer functional - visit the dashboard to re-authorize tapiriik.",
      "full": "Your %(service) account is full. Make some space available then visit the dashboard to re-synchronize.",
      "expired": "Your %(service) account has expired. Once it's back in action, visit the dashboard to re-synchronize.",
      "unpaid": "You must have a paid account with %(service) in order to synchronize activities.",
      "flow": "You've excluded this activity from synchronizing to %(service).",
      "private": "This activity is private and will not synchronize to %(service).",
      "notrigger": "%(service) is only synchronized when new activities are available", // I have nooo clue why I made this error, keeping it for posterity.
      "credentials_missing": "You did not opt to remember the credentials for %(service).",
      "config_missing": "%(service) requires configuration.",
      "stationary": "%(service) does not support stationary activity upload.",
      "type_unsupported": "%(service) does not support this type of activity.",
      "download": "An error occured when retrieving the activity data to upload to %(service).",
      "list": "There was a problem indexing your activities on %(service), so no activities will be uploaded to %(service).",
      "upload": "There was a problem uploading this activity to %(service).",
      "sanity": "This activity contains unusual data that is most likely incorrect.",
      "corrupt": "This activity is missing data required for synchronization.",
      "untagged": "This activity is not tagged with its activity type.",
      "live": "This activity hasn't been completed yet.",
      "tz_unknown": "The time zone of this activity could not be determined.",
      "system": "There was a system error while synchronizing this activity.",
      "other": "There was an error while synchronizing this activity.",
      "unknown": "Your guess is as good as mine."
    };
    return explanations[type].replace("%(service)", $scope.DisplayNameByService(presc.Service));
  };

  var loadActivities = function(pageStartDate) {
    loading = true;
    $http.get("/activities/fetch")
    .success(function(activities) {
      loading = false;
      for (var actidx in activities){
        var activity = activities[actidx];
        // Convert dict to an array sorted by the display order.
        var fully_synchronized = true;
        var sorted_prescences = [];
        for (var svcidx in tapiriik.ServiceInfo){
          if (!tapiriik.ServiceInfo[svcidx].Connected) continue;
          // Expand the "otherwise" entry.
          if (activity.Prescence[svcidx] === undefined){
            activity.Prescence[svcidx] = activity.Prescence[""];
          }
          if (activity.Prescence[svcidx].Exception) fully_synchronized = false;
          activity.Prescence[svcidx].Present = activity.Prescence[svcidx].Exception === undefined;
          activity.Prescence[svcidx].Service = svcidx;
          sorted_prescences.push(activity.Prescence[svcidx]);
        }
        activity.FullySynchronized = fully_synchronized;
        activity.Prescence = sorted_prescences;
      }
      $scope.activities = activities;
    });
  };

  $scope.isLoading = function() {
    return loading;
  };

  loadActivities();
}

angular.module('tapiriik', []).config(function($interpolateProvider) {
  $interpolateProvider.startSymbol('{[').endSymbol(']}');
});
