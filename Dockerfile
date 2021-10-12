FROM tomcat:9.0.37
# COPY path-to-your-application-war path-to-webapps-in-docker-tomcat
COPY target/dataBase.war /usr/local/tomcat/webapps/
COPY tomcat-users.xml /usr/local/tomcat/conf/
EXPOSE 8080
ENV JAVA_OPTS="-Xmx8192m -DprojectPath=/usr/local/database/"