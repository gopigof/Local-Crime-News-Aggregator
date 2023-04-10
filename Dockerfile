FROM sbtscala/scala-sbt:eclipse-temurin-focal-11.0.17_8_1.8.2_2.13.10

RUN mkdir -p /root/build/project
ADD build.sbt /root/build/
ADD ./project/plugins.sbt /root/build/project
RUN cd /root/build && sbt compile

#EXPOSE 9000
WORKDIR /root/build

CMD sbt compile run