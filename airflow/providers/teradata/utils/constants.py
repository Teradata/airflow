class Constants:
    CC_SUSPEND_OPR = "SUSPEND"
    CC_RESUME_OPR = "RESUME"
    CC_INITIALIZE_DB_STATUS = "Initializing"
    CC_SUSPEND_DB_STATUS = "Suspended"
    CC_RESUME_DB_STATUS = "Running"
    CC_OPR_SUCCESS_STATUS_MSG = "Compute Cluster %s  %s operation completed successfully."
    CC_OPR_FAILURE_STATUS_MSG = "Compute Cluster %s  %s operation has failed."
    CC_OPR_INITIALIZING_STATUS_MSG = "The environment is currently initializing. Please wait."
    CC_OPR_EMPTY_PROFILE_ERROR_MSG = "Please provide a valid name for the compute cluster profile."
    CC_GRP_PRP_NON_EXISTS_MSG = "The specified Compute cluster is not present or The user doesn't have permission to access compute cluster."
    CC_GRP_PRP_UN_AUTHORIZED_MSG = "The %s operation is not authorized for the user."
    CC_GRP_LAKE_SUPPORT_ONLY_MSG = "Compute Groups is supported only in Vantage Cloud Enterprise."
    CC_OPR_TIME_OUT = 1200
    CC_POLL_INTERVAL = 60
