
/**
 * Module dependencies.
 */

var express = require('express')
  , routes = require('./routes')
  , user = require('./routes/user')
  , http = require('http')
  , home =require('./routes/home')
  , path = require('path')
  , admin = require('./routes/user')
  ,user = require('./routes/user');

var app = express();

// all environments
app.set('port', process.env.PORT || 3019);
app.set('views', __dirname + '/views');
app.set('view engine', 'ejs');
app.use(express.favicon());
app.use(express.logger('dev'));
app.use(express.bodyParser());
app.use(express.methodOverride());
app.use(app.router);
app.use(express.static(path.join(__dirname, 'public')));

// development only

app.get('/', home.signin);
app.get('/logout',home.signin);
app.post('/afterSignIn', home.afterSignIn);
app.post('/signup', home.signup);
app.post('/afterSignup', home.afterSignup);
app.get('/register',user.getRegisterPage)
app.get('/typography',admin.getTopography);
app.get('/index',admin.getIndex);
app.get('/alerts',admin.getAlert);
app.get('/showUsers',admin.showUsers);
app.get('/pdContacts',admin.getpdContacts);
app.post('/alertPeople',admin.getAlertPeople);
app.post('/getRegister',admin.getRegister);


http.createServer(app).listen(app.get('port'), function(){
  console.log('Express server listening on port ' + app.get('port'));
});
