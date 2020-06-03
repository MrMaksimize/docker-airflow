#!/usr/bin/env R
library(rsconnect)

suppressMessages(library(docopt))       # we need docopt (>= 0.3) as on CRAN

doc <- "Usage: shiny_deploy.r [-a SHINYAPPS_APPNAME] [-n SHINYAPPS_NAME] [-t SHINYAPPS_TOKEN] [-h] [-s SHINYAPPS_SECRET] [-f FORCE_UPDATE] [-p SHINYAPP_PATH]

-a --appname SHINYAPPS_APPNAME SHINYAPPS APPNAME
-n --name SHINYAPPS_NAME      SHINYAPPS USER
-t --token SHINYAPPS_TOKEN    SHINYAPPS TOKEN
-s --secret SHINYAPPS_SECRET  SHINYAPPS SECRET
-f --force FORCE_UPDATE       FORCE UPDATE
-p --path SHINYAPP_PATH       SHINY APP PATH
-h --help           show this help text"

opt <- docopt(doc)


rsconnect::setAccountInfo(name=opt$name, token=opt$token, secret=opt$secret)
deployApp(opt$path, launch.browser=FALSE, forceUpdate = opt$force, appName = opt$appname)