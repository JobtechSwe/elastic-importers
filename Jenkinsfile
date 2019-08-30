pipeline {
    agent any
    environment {
        scannerHome = tool 'Jobtech_Sokapi_SonarScanner'
        version = "1"
        buildTag = "${version}.${BUILD_NUMBER}"
        buildName= "elastic-importers"
    }
    stages{
        stage('Checkout code'){
            steps{
                checkout scm: [
                    $class: 'GitSCM'
                ]               
            }
        }
        stage('Code analysis'){
            steps {
                withSonarQubeEnv('Jobtech_SonarQube_Server'){
                sh "${scannerHome}/bin/sonar-scanner -Dsonar.projectKey=${buildName} -Dsonar.sources=."
                }
            }
        }
        stage('Build and Tag Openshift Image'){
            steps{
                openshiftBuild(namespace:'${openshiftProject}', bldCfg: '${buildName}', showBuildLogs: 'true')
                openshiftTag(namespace:'${openshiftProject}', srcStream: '${buildName}', srcTag: 'latest', destStream: '${buildName}', destTag:'${buildTag}')
            }
        }
        stage('Change Cronjob Images'){
            steps{
                sh "oc patch cronjobs/import-taxonomy --type=json -p='[{\"op\":\"replace\", \"path\": \"/spec/jobTemplate/spec/template/spec/containers/0/image\", \"value\":\"docker-registry.default.svc:5000/${openshiftProject}/${buildName}:${buildTag}\"}]' -n ${openshiftProject}"
                sh "oc patch cronjobs/import-platsannonser --type=json -p='[{\"op\":\"replace\", \"path\": \"/spec/jobTemplate/spec/template/spec/containers/0/image\", \"value\":\"docker-registry.default.svc:5000/${openshiftProject}/${buildName}:${buildTag}\"}]' -n ${openshiftProject}"
            }
        }
    }
    post {
        success {
            slackSend color: 'good', message: "${GIT_URL}, Branch: ${GIT_BRANCH}, Commit: ${GIT_COMMIT} successfully built to project ${openshiftProject} build: ${buildTag}."
        }
        failure {
            slackSend color: '#FF0000', channel: '#narval-sokapi', message: "${GIT_URL} ${GIT_BRANCH} ${GIT_COMMIT} FAILED to build to ${openshiftProject} build ${buildTag}."
        }
        unstable {
            slackSend color: '#FFFF00', message: "${GIT_URL} ${GIT_BRANCH} ${GIT_COMMIT} unstable build for ${openshiftProject} build ${buildTag}."
        }
    }
}
