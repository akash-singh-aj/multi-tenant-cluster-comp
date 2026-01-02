plugins {
    id("java")
    id("application")
}

group = "com.flo.common"
version = "1.0-SNAPSHOT"

application {
    mainClass.set("com.flo.app.Main")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(platform("com.typesafe.akka:akka-bom_2.13:2.6.20"))
    implementation("com.typesafe.akka:akka-actor-typed_2.13")
    implementation("com.typesafe.akka:akka-cluster-typed_2.13")
    implementation("com.typesafe.akka:akka-serialization-jackson_2.13")
    implementation("ch.qos.logback:logback-classic:1.4.14")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("com.typesafe.akka:akka-actor-testkit-typed_2.13")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<JavaExec> {
    standardInput = System.`in`
}

tasks.test {
    useJUnitPlatform()
}