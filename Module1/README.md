## Learning about Docker
Docker is a platform for building, running, and sharing containerized applications.

Docker lets one to package code plus dependencies into lightweight, isolated containers, which makes projects reproducible and easy to run anywhere.

`hello-world` is a docker image. Image is an immutable template used to create containers. In particular, `docker run hello-world` is the standard sanity check that Docker is installed and can pull and run containers correctly.

Similarly, `docker run -it ubuntu bash` starts a new Ubuntu container and drops an interactive bash shell inside that container. We can install tools or test commands without touching our host OS.

Let's run 'docker run -it python:3.13.11-slim' which launches an interactive Python 3.13.11 shell in a lightweight Docker container based on Debian slim.