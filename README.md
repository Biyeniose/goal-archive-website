# Goal Archive web app

> #### Frontend
>
> - `cd goal_archive`
> - `npm install`
> - `npm run dev`
> - App runs on `localhost:3000`

---

## Backend server

> #### Backend
>
> - `cd api`

<details>
<summary>Docker instructions</summary>

List all images

```
docker images
```

Remove Dangling Images Only:

```
docker image prune
```

Remove All Unused Images (Not Associated with Any Container)

```
docker image prune -a
```

Build the image

```
docker build -t myimage .
```

Run the container (add -d in front for detached mode):

```
docker run --name mycontainer -p 90:90 myimage
```

Stop and remove container:

```
docker stop mycontainer
docker rm mycontainer
```

list all Docker containers, including both running and stopped ones (remove a for only running):

```
docker ps -a
```

Delete All Unused Containers (same for images)

```
docker container prune
```

To clean up unused containers, networks, and volumes as well:

```
docker system prune
```

## Docker compose

Build images

```
docker compose build
```

Start containers

```
docker compose up
```

Or combine

```
docker-compose up --build
```

Stop containers

```
docker compose stop
```

Remove containers

```
docker compose down
```

List running containers

```
docker compose ps
```

Access terminal of already running container

```
docker exec -it mycontainer /bin/bash
```

</details>
