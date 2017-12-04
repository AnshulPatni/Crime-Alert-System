/*
 * GET home page.
 */
var ejs = require('ejs');
var mysql = require('./mysql');

function signin(req, res) {
    console.log("IN SIGNIN");
    res.render('signin',{error:""});
   /* ejs.renderFile('./views/signin.ejs', function (err, result) {
        // if it is success
        if (!err) {
            res.end({result:result,error:""});
        }
        //ERROR
        else {
            res.end("ERROR OCCURED");
            console.log(err);
        }
    });*/
}

function signup(req, res) {
    ejs.renderFile('./views/signup.ejs', function (err, result) {
        // if it is success
        if (!err) {
            res.end(result);
        }
        //ERROR
        else {
            res.end("ERROR IN SIGNUP OCCURED");
            console.log(err);
        }
    });
}

function afterSignup(req, res) {

    console.log("After Signup");
    var getuser = "select * from users where username='" + req.param("username") + "'";
    mysql.fetchData(function (err, result) {
        if (err) {
            throw err;
        }
        else if (result.length > 0) {
            console.log(result[0].username);
            var error = result[0].username + " already exist please try another username"
            console.log(error);
            res.render('register', {error: error});
        }
        else {

            var userDetails = "insert into users(username,password,name,city,email,contactNO,isAdmin,isRegister) values('" +
                req.param('username') + "','" + req.param('password') + "','" + req.param('name') +
                "','" + req.param('city') + "','" + req.param('email') + "','" + req.param('ContactInfo') +
                "','No','No')";
            mysql.fetchData(function (err, result) {
                if (err) {
//		if(err==='Error')
//			res.render('alreadycreated');
//		//else
                    throw err;
                }
                else {
                    res.render('signin',{error:""});
                    console.log("Account Created");
                    console.log(req.param('name'));
                }
            }, userDetails);
        }
    }, getuser);


}


function afterSignIn(req, res) {
    console.log("After Signin");
    var getuser = "select * from users where username='" + req.param("username") + "' and password='" + req.param("password") + "'";
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
                    var getAlert = "select * from ALERTS where city = '"+result[0].forCity+"'";
                    mysql.fetchData(function (err, result2) {
                        console.log("user  result", result[0]);
                        console.log("alert  result", result2);
                        res.render('userDashboard', {userData: result[0],alertData:result2});
                    }, getAlert);
                }
            }
            else {
                //for invalid login
                var error = 'Login Failed';
                res.render('signin',{error:error});
            }
        }
    }, getuser);

}

exports.signin = signin;
exports.signup = signup;
exports.afterSignIn = afterSignIn;
exports.afterSignup = afterSignup;