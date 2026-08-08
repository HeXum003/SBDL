pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
               sh 'pipenv --python python sync'
            }
        }
        stage('Test') {
            steps {
               sh 'pipenv run pytest'
            }
        }
        stage('Package') {
	    when{
		    anyOf{ branch "master" ; branch 'release' }
	    }
            steps {
               sh 'python -m zipfile -c sbdl.zip lib'
            }
        }
    }
}
