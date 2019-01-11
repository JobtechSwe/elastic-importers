#!groovy

// Run this pipeline on the custom Jenkins Slave ('jobtech-appdev')
// Jenkins Slaves have JDK and Maven already installed
// 'jobtech-appdev' has skopeo installed as well.
node('jobtech-appdev'){

  // The following variables need to be defined at the top level and not inside the scope of a stage - otherwise they would not be accessible from other stages.
  def version    = "1"
  //def chechoutDir = "/tmp/workspace/importers-pipeline"

  // Set the tag for the development image: version + build number
  def devTag  = "${version}-${BUILD_NUMBER}"
  
  def branchName = env.BRANCH_NAME;

  // Checkout Source Code
  stage('Checkout Source') {
  echo "Branch is: ${env.BRANCH_NAME}"
    checkout scm
    echo "Branch Name: ${branchName}"
  }

  // Call SonarQube for Code Analysis
  stage('Code Analysis') {
    echo "Running Code Analysis"
    // requires SonarQube Scanner 2.8+
    def scannerHome = tool 'Jobtech_Sokapi_SonarScanner';
    echo "Scanner Home: ${scannerHome}"
    ////sh "${scannerHome}/bin/sonar-scanner -Dsonar.projectKey=jobtech_sokapi -Dsonar.sources=. -Dsonar.host.url=http://sonarqube-jt-sonarqube.dev.services.jtech.se -Dsonar.login=${sonarqube_token}"
    withSonarQubeEnv('Jobtech_SonarQube_Server') {
      sh "${scannerHome}/bin/sonar-scanner -Dsonar.projectKey=sokapi_sonar -Dsonar.sources=."
    }
  }

  // Build the OpenShift Image in OpenShift, tag and pus to nexus.
  stage('Build and Tag OpenShift Image') {
    echo "Building OpenShift container image elastic-importers:${devTag}"

    // Start Binary Build in OpenShift using the file we just published
    sh "oc start-build elastic-importers -n sokannonser-develop --follow"

    // Tag the image using the devTag
    sh "oc tag jt-dev/sokapi:latest jt-dev/sokapi:${devTag} -n jt-dev"
  }

  // Deploy the built image to the Development Environment.
  stage('Deploy to Dev Env') {
    echo "Deploying container image to Development Env Project"

    echo "DEV TAGGING"
    sh "oc tag sokannonser-develop/elastic-importers:latest sokannonser-develop/elastic-importers:${devTag} -n sokannonser-develop"
  }
}
