var mysql = require('./mysql');
var AWS = require('aws-sdk');

// AWS.config.update({
//     accessKeyId: "",
//     secretAccessKey: "",
//     region:'us-west-2',
//     sslEnabled: true
// });

AWS.config.update({
    accessKeyId: "xxxxxx",
    secretAccessKey: "xxxxx",
    region:'us-west-2',
    sslEnabled: true
});


var payload = {
    default: 'Hello World',
    APNS: {
        aps: {
            alert: 'Hello World',
            sound: 'default',
            badge: 1
        }
    }
};

/*
 * GET users listing.
 */

exports.list = function(req, res){
    res.render('user');
};
exports.getRegisterPage= function (req,res) {
    res.render('register',{error:""});
};

exports.getTopography = function (req,res) {
    res.render('pages/typography');
};
exports.getIndex = function (req,res) {
    res.render('pages/index');
};
exports.getpdContacts = function (req,res) {
    res.render('pages/pdContacts');
};




exports.getAlert = function (req,res) {
    var getAlerts = "select * from ALERTS ORDER BY ID Desc";
    mysql.fetchData(function (err, result) {
        if (err) {
            throw err;
        }
        else if (result.length > 0) {
           // console.log(JSON.stringify(result));
            res.render('pages/alerts',{alertData:result});
        }
    }, getAlerts);
    // res.render('pages/alerts');
};


exports.showUsers = function (req,res) {
    var showSFO = "select Name,city,email,contactNO from users where forCity='SFO'";
    var showLA="select Name,city,email,contactNO from users where forCity='LA'";
    mysql.fetchData(function (err, result) {
        mysql.fetchData(function (err1,result1) {

        if (err) {
            throw err;
        }
        else if (result1.length > 0) {
            console.log(JSON.stringify(result));
            res.render('pages/showUsers',{alertSFO:result,alertLA:result1});
         }

         }, showLA);

    },showSFO);

    // res.render('pages/alerts');
};



exports.getRegister=function (req,res) {

console.log(req.body.id);
 console.log(req.body);
    console.log(req.body.city);
//console.log(req.body.contactnumber);
console.log(req.body.userID);

    req.body.contactnumber='+1'+req.body.contactnumber;
    console.log(req.body.contactnumber);


    var sns = new AWS.SNS();

    if(req.body.sms=='SMS') {
    console.log("IN SMS");

    if(req.body.city=='SFO') {
        var params = {
            Protocol: 'sms', /* required */
            TopicArn: 'xxxxxxx', /* required */
            Endpoint: req.body.contactnumber
        };
    }
    else
    {
        var params = {
            Protocol: 'sms', /* required */
            TopicArn: 'xxxxxxx', /* required */
            Endpoint: req.body.contactnumber
        };
    }
        
    sns.subscribe(params, function (err, data) {
        if (err) {
            console.log(err.stack);
        } else {
            console.log('push sent');
            console.log(data);
        }
        // if (err) console.log(err, err.stack); // an error occurred
        // else     console.log(data);           // successful response
    });
}

if(req.body.email1)
{

    if(req.body.city=="SFO") {
        var params = {
            Protocol: 'email', /* required */
            TopicArn: 'xxxxxx', /* required */
            Endpoint: req.body.id
        };
    }
    else
    {
        var params = {
            Protocol: 'email', /* required */
            TopicArn: 'xxxxx', /* required */
            Endpoint: req.body.id
        };
    }

    sns.subscribe(params, function(err, data) {
        if (err) {
            console.log(err.stack);
        }else{
            console.log('push sent');
            console.log(data);
        }
        // if (err) console.log(err, err.stack); // an error occurred
        // else     console.log(data);           // successful response
    });
  }

    var updateStatus="update users set isRegister='YES',forCity='"+req.body.city+"' where email='"+req.body.id+"'";

    mysql.fetchData(function (err,result1) {

        if(!err)
        {
            console.log("Updated the record in DB");
            var getuser = "select * from users where username='" + req.body.userID + "'";
            mysql.fetchData(function (err, result) {
                if (err) {
                    throw err;
                }
                else {
                    if (result.length > 0) {
                        console.log(result[0].username);
                        if (result[0].isAdmin.toLowerCase() === 'y') {
                            res.render('pages/index');
                        }
                        else {
                            console.log("inside city");
                            var getAlert = "select * from ALERTS where city = '"+req.body.city+"'";
                            mysql.fetchData(function (err, result2) {
                                console.log("user  result", result[0]);
                                console.log("alert  result", result2);
                                res.render('userDashboard', {userData: result[0],alertData:result2});
                            }, getAlert);
                        }
                    }
                }
            }, getuser);
        }
        else
            console.log(err);
    }, updateStatus);



};



exports.getAlertPeople = function (req,res) {
    //console.log("body has",req.body);
//   console.log(req.body);
    console.log(req.body.id);
//   res.render('success');
    res.render('pages/index');
    //res.render('pages/alerts');
    var sns = new AWS.SNS();
    var payload = {
        default: 'Hello World',
        APNS: {
            aps: {
                alert: 'Hello World',
                sound: 'default',
                badge: 1
            }
        }
    };
    var message=[];
    var payload={};
    var text='';
    var parsedData=[];
    var getAlerts = "select * from ALERTS where ID="+req.body.id+"";
    mysql.fetchData(function (err, result) {
        if (err) {
            throw err;
        }
        else if (result.length > 0) {

            var updateStatus="update ALERTS set Notified='YES' where ID="+req.body.id+"";
            mysql.fetchData(function (err,result1) {
                if(!err)
                {

                    console.log("Updated the record in DB");
                }
            }, updateStatus);

            message=JSON.stringify(result);
            parsedData = JSON.parse(message);
            console.log(parsedData[0].City);
            // res.render('pages/alerts',{alertData:result});
                text='ALERT!!! There is '+ parsedData[0].Category+ ': '+ parsedData[0].Descript+', '+parsedData[0].City+' at '+parsedData[0].Address + ' on '+parsedData[0].CrimeDate+' at '+parsedData[0].CrimeTime+'. Please be careful!';
            if(parsedData[0].City=="SFO") {
                var params = {
                    Message: text,
                    MessageStructure: 'string',
                    TargetArn: 'xxxxxx'
                };
            }
            else
            {
                var params = {
                    Message: text,
                    MessageStructure: 'string',
                    TargetArn: 'xxxxxx'
                };
            }

            sns.publish(params, function(err, data) {
                if (err) {
                    console.log(err.stack);
                    return;
                }
                console.log('push sent');
                console.log(data);
            });
        }
    }, getAlerts);

};