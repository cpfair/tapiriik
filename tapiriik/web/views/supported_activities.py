from django.shortcuts import render


def supported_activities(req):
    # so as not to force people to read REGEX to understand what they can name their activities
    ELLIPSES = "&hellip;"
    activities = {}
    activities["Running"] = ["run", "running"]
    activities["Cycling"] = ["cycling", "cycle", "bike", "biking"]
    activities["Mountain biking"] = ["mtnbiking", "mtnbiking", "mountainbike", "mountainbiking"]
    activities["Walking"] = ["walking", "walk"]
    activities["Hiking"] = ["hike", "hiking"]
    activities["Downhill skiing"] = ["downhill", "downhill skiing", "downhill-skiing", "downhillskiing", ELLIPSES]
    activities["Cross-country skiing"] = ["xcskiing", "xc-skiing", "xc-ski", "crosscountry-skiing", ELLIPSES]
    activities["Roller skiing"] = ["rollerskiing"]
    activities["Snowboarding"] = ["snowboarding", "snowboard"]
    activities["Skating"] = ["skate", "skating"]
    activities["Swimming"] = ["swim", "swimming"]
    activities["Wheelchair"] = ["wheelchair"]
    activities["Rowing"] = ["rowing", "row"]
    activities["Elliptical"] = ["elliptical"]
    activities["Other"] = ["other", "unknown"]
    activityList = []
    for act, synonyms in activities.items():
        activityList.append({"name": act, "synonyms": synonyms})
    return render(req, "supported-activities.html", {"actMap": activityList})
