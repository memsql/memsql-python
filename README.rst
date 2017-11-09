=======================
MemSQL Python Libraries
=======================

This library contains various plugins and wrappers designed by MemSQL
engineers for a couple of important python libraries.

Install
=======

.. code:: bash

    pip install memsql

Copy and paste the following steps to get started quickly on Ubuntu:

.. code:: bash

    sudo apt-get update
    sudo apt-get install -y mysql-client python-dev libmysqlclient-dev python-pip
    sudo pip install memsql
    
Copy and paste the following to get 
started with RHEL based distributions such as Amazon Linux or Centos:
.. code:: bash

    sudo yum update
    sudo yum install -y gcc mysql-devel
    sudo pip install memsql

Testing
=======

.. image:: https://travis-ci.org/memsql/memsql-python.png
    :target: https://travis-ci.org/memsql/memsql-python

Run tests by executing :code:`make test`.
