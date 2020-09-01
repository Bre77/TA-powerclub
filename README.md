# TA-powerclub
Powerclub Usage and Price data for Splunk using a metrics index.

## inputs.conf
    [powerclub://myusage]
    email = 
    password = 
    disabled = 0
    # Index must be a Metrics Index
    index = 
    # Dont make your interval too fast or Powerclub will reject your requests
    interval = 14400
