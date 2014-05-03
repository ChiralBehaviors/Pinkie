Pinkie - a minimal NIO socket channel handler

Pinkie is licenced under the Apache License, Version 2.0

This small library provides a minimal, but full featured NIO socket channel handler.  Pinkie provides a generic selectable ChannelHandler for both inbound and outbound sockets.  The library provides the concrete handling of SocketChannels, as well as a ServerSocketChannel handler.  Pinkie uses a factory to create instances of the event handlers for SocketChannel events.

Pinkie requires Maven 3.x to build.  To build Pinkie, cd into the root directory and do:

    $ mvn clean install
    
For examples on how to use Pinkie, see the unit tests.  For an example of how to use Pinkie and the [Tron Finite State Machine Model](https://github.com/Hellblazer/Tron), see the [Pinkie FSM example](https://github.com/Hellblazer/pinkie-fsm-example) project

See the [Pinkie wiki](https://github.com/Hellblazer/Pinkie/wiki) for usage and design notes.

For prebuilt versions of Pinkie, add the Hellblazer cloudbees repositories to your project's pom.xml:

For snapshots: 

    <repository>
        <id>hellblazer-snapshots</id>
        <name>Hellblazer Snapshots </name>
        <url>http://repository-hal900000.forge.cloudbees.com/snapshot/</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
    
For releases: 

    <repository>
        <id>hellblazer-releases</id>
        <name>Hellblazer Releases </name>
        <url>http://repository-hal900000.forge.cloudbees.com/release/</url>
    </repository>

The current released version of Pinkie is 0.0.3.  To use Pinkie in your project, add the following dependency in your project's pom.xml:


    <dependency>
        <groupId>com.hellblazer</groupId>
        <artifactId>pinkie</artifactId>
        <version>0.0.3</version>
    </dependency>

The current snapshot version of Pinkie is 0.0.4-SNAPSHOT.  To use Pinkie in your project, add the following dependency in your project's pom.xml:


    <dependency>
        <groupId>com.hellblazer</groupId>
        <artifactId>pinkie</artifactId>
        <version>0.0.4-SNAPSHOT</version>
    </dependency>
    

