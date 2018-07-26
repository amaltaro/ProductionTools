FROM cern/cc7-base
LABEL maintainer="Alan Malta (alan.malta@cern.ch)"

# User cmst1:zh to be used to run the container
ENV _USER=31961
ENV _GRP=1399

RUN echo "Create /data and make sure cmst1 can read/write to it"
RUN mkdir /data
RUN chown root:${_GRP} /data
RUN chmod 775 /data

#RUN echo "Setting up cmst1:zh user"
#USER ${_USER}:${_GRP}

ENV WORK_DIR=/data/srv
ENV ADMIN_DIR=/data/admin/wmagent
ENV SECRETS_DIR=/data/certs
ENV CURRENT_DIR=/data/srv/wmagent/current

RUN echo "Creating required directories"
RUN ls -la
RUN mkdir -p $WORK_DIR
RUN mkdir -p $ADMIN_DIR
RUN mkdir -p $SECRETS_DIR
RUN mkdir -p $CURRENT_DIR

RUN echo "Copying files over the filesystem of the image"
# Set ownership/group to cmst1/zh
ADD --chown=31961:1399 env.sh ${ADMIN_DIR}/
ADD --chown=31961:1399 WMAgent.secrets ${ADMIN_DIR}/
ADD --chown=31961:1399 deploy-wmagent.sh ${WORK_DIR}/

RUN echo "Creating dummy grid certificate files"
WORKDIR $SECRETS_DIR
RUN touch myproxy.pem servicecert.pem servicekey.pem
RUN chmod 600 *
RUN chown ${_USER}:${_GRP} *

# Add the extra system stuff we need
RUN echo "Updating the base system"
RUN yum update -y && yum clean all

RUN echo "Install the platform seeds"
RUN yum install -y glibc coreutils bash tcsh zsh perl tcl tk readline openssl ncurses e2fsprogs krb5-libs freetype ncurses-libs perl-libs perl-ExtUtils-Embed \
        fontconfig compat-libstdc++-33 libidn libX11 libXmu libSM libICE libXcursor \
        libXext libXrandr libXft mesa-libGLU mesa-libGL e2fsprogs-libs libXi libXinerama libXft-devel \
        libXrender libXpm libcom_err perl-Test-Harness perl-Carp perl-constant perl-PathTools \
        perl-Data-Dumper perl-Digest-MD5 perl-Exporter perl-File-Path perl-File-Temp perl-Getopt-Long \
        perl-Socket perl-Text-ParseWords perl-Time-Local libX11-devel libXpm-devel libXext-devel mesa-libGLU-devel \
        perl-Switch perl-Storable perl-Env perl-Thread-Queue nspr nss nss-util file file-libs readline \
        zlib popt bzip2 bzip2-libs

RUN echo "Install packages required by WMAgent"
RUN yum install -y git-core krb5-devel readline-devel openssl libXft-devel libXpm-devel libXext-devel mesa-libGLU-devel perl-Switch perl-Env perl-Thread-Queue
RUN yum install -y perl wget unzip
RUN yum clean all & rm -rf /var/cache/yum

# start the setup and WMAgent installation
WORKDIR ${WORK_DIR}

#ADD config.json $WORK_DIR/config.json
#ENV PKG_CONFIG_PATH=$WORK_DIR
#RUN sed -i -e "s,_ \"github.com/go-sql-driver/mysql\",,g" web/server.go
#RUN cat $WDIR/config.json | sed -e "s,GOPATH,$GOPATH,g" > dbsconfig.json
#ENV PATH="${GOPATH}/src/github.com/vkuznet/dbs2go:${GOPATH}/src/github.com/vkuznet/dbs2go/bin:${PATH}"
#ENV X509_USER_PROXY=/etc/secrets/dbs-proxy

#RUN sh $WORK_DIR/deploy-wmagent.sh 
RUN sh $WORK_DIR/deploy-wmagent.sh -w 1.1.12.patch3 -d HG1804d -t testbed-alan -n 0 -c cmsweb-testbed.cern.ch

#VOLUME /home/dmwm/unittestdeploy/wmagent/current/install/

#ENTRYPOINT ["TestScripts/runSlice.sh"]

