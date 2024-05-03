# libpredweb
This is the library for the protein prediction servers used by the following repos

https://github.com/NBISweden/predictprotein-webserver-topcons2
https://github.com/NBISweden/predictprotein-webserver-proq3
https://github.com/NBISweden/predictprotein-webserver-scampi2
https://github.com/NBISweden/predictprotein-webserver-boctopus2
https://github.com/NBISweden/predictprotein-webserver-pconsc3
https://github.com/NBISweden/predictprotein-webserver-prodres
https://github.com/NBISweden/predictprotein-webserver-subcons
https://github.com/NBISweden/predictprotein-webserver-common-backend


## How to install 

Use the following command to install the package with `pip`

```bash
pip install git+https://github.com/nanjiangshu/libpredweb.git@main
```
## Usage

Add the following lines to your Python script in order to use the library
```python
from libpredweb import myfunc
from libpredweb import webserver_common as webcom
from libpredweb import qd_fe_common as qdcom
```
