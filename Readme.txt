we need to have access to the paid version of nginx to manage the load balancing even more

to run diffrent instances of the app use :

waitress-serve --port=800x app:app

you'll also need to install nginx on your pc and modif the nginc.conf file to set the load balancing 
