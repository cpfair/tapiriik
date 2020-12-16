require('cross-fetch/polyfill');
const fs = require('fs');
const { Api, MobileApi } = require('endomondo-api-handler');

const api = new Api();
// const mobileApi = new MobileApi();

const { DateTime } = require('luxon');

var express = require("express");
var app = express();
app.listen(8001, () => {
    console.log("Server running on port 8001");
});

async function auth(login, password) { 
    
    let token = await api.login(login, password);
    
    if (token) {
        return {
            "token": token,
            "user-id": api.getUserId()
        };
    }
    return false;
}

async function getWorkouts(login, password, date_from, date_to, limit, offset) { 
    
    await api.login(login, password);
    
    const { workouts, paging } = await api.getWorkouts({
        after: DateTime.fromFormat(date_from, "y-M-d"),
        before: DateTime.fromFormat(date_to, "y-M-d"),
        limit: limit,
        offset: offset
    });

    return { workouts, paging };
}

async function getWorkout(login, password, workoutID) { 
    
    await api.login(login, password);
    
    const workout = await api.getWorkout(workoutID);
    return workout;
}

async function getWorkoutGPX(login, password, workoutID) { 
    
    await api.login(login, password);
    
    const workoutGPX = await api.getWorkoutGpx(workoutID);
    return workoutGPX;
}

async function getWorkoutTCX(login, password, workoutID) { 

    await api.login(login, password);
  
    const workoutTCX = await api.getWorkoutTcx(workoutID)
    return workoutTCX;
}

app.all("/workouts", (req, response) => {
    
    let user = req.query.user;
    let pass = req.query.pass;
    let date_from = req.query.date_from;
    let date_to = req.query.date_to;

    let offset = (req.query.offset !== undefined) ? req.query.offset : 0;
    let limit = (req.query.limit !== undefined) ? req.query.limit : 50;
    
    if (user !== undefined && pass !== undefined && date_from !== undefined && date_to !== undefined) {
        getWorkouts(user, pass, date_from, date_to, limit, offset)
        .then(function (data) {
            response.status(200).json(data);
        })
        .catch(function (e) {
            console.log(e);
            response.status(500, {
                error: e
            });
        });
    } else {
        console.log("Error: Missing parameters");
        response.status(500).send("Error: Missing parameters");
    }
})

app.all("/workout/:workout", (req, response) => {

    let user = req.query.user;
    let pass = req.query.pass;
    let workoutID = req.params.workout;

    if (user !== undefined && pass !== undefined && workoutID !== undefined) {
        getWorkout(user, pass, workoutID)
        .then(function (data) {
            response.status(200).json(data);
        })
        .catch(function (e) {
            console.log(e);
            response.status(500, {
                error: e
            });
        });
    } else {
        console.log("Error: Missing parameters");
        response.status(500).send("Error: Missing parameters");
    }
})

app.all("/workout/:workout/gpx", (req, response) => {

    let user = req.query.user;
    let pass = req.query.pass;
    let workoutID = req.params.workout;

    if (user !== undefined && pass !== undefined && workoutID !== undefined) {
        getWorkoutGPX(user, pass, workoutID)
        .then(function (data) {
            response.status(200).send(data);
        })
        .catch(function (e) {
            console.log(e);
            response.status(500, {
                error: e
            });
        });
    } else {
        console.log("Error: Missing parameters");
        response.status(500).send("Error: Missing parameters");
    }
})

app.all("/workout/:workout/tcx", (req, response) => {

    let user = req.query.user;
    let pass = req.query.pass;
    let workoutID = req.params.workout;

    if (user !== undefined && pass !== undefined && workoutID !== undefined) {
        getWorkoutTCX(user, pass, workoutID)
        .then(function (data) {
            response.status(200).send(data);
        })
        .catch(function (e) {
            console.log(e);
            response.status(500, {
                error: e
            });
        });
    } else {
        console.log("Error: Missing parameters");
        response.status(500).send("Error: Missing parameters");
    }
})

app.all("/auth", (req, response) => {

    let user = req.query.user;
    let pass = req.query.pass;

    if (user !== undefined && pass !== undefined) {
        auth(user, pass)
            .then(function (data) {
                response.status(200).json(data);
            })
            .catch(function (e) {
                console.log(e);
                response.status(500).json({
                    error: e
                });
            });
    } else {
        console.log("Error: Missing credentials");
        response.status(500).send("Error: Missing credentials");
    }
})

