#  $Header     : File Name & Version : XXDBD_LEARN_PRC_LINK.sh  v 1.1$
#  Author      : Vidyadhar Kodedala
#  Description : This shell script will set the environment
#                and creates symbolic link for the script XXDBD_LEARN_PRC.prog.
#                Login as <SID>Manager and execute this script.
#  Dependencies: None						  
                    
#------------------------------------------------------------------------
#. $APPL_TOP/APPSORA.env

ln -s $FND_TOP/bin/fndcpesr $XXDBDPER_TOP/bin/XXDBD_LEARN_PRC

RC=$?
if [[ $RC != 0 ]]; then
    echo "ERROR in Symolic link creation !!!"
    exit $RC
else
   echo "Symolic link successfully created"
fi
