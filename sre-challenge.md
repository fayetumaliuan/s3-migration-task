# SRE test

Thank you for taking the time to take this test. You can solve it at your own pace.

Please don't hesitate to contact us via email with whatever question you have about it.

## Legacy asset move

Let's say we have backend service whose mission is to store users avatars (in PNG files) by uploading them to S3 and then their S3 paths (without the domain part that shows the S3 bucket name) is eventually stored in a SQL database table for future reference. These paths are used to show these PNG files in the frontend.

Let's say that for historical reasons, in the past, all these PNG files were stored in one bucket with one path (directory structure) (from now on `legacy-s3`) and after some time the bucket changed and the path within the bucket changed as well (from now on `production-s3`). Which means, half of the PNGs live now inside one bucket with one path and the other half live in a different bucket with a different path. And, of course, this means that the database table contains a mix of paths.

Example:

- One legacy PNG would have a URL like https://legacy-url/image/avatar-32425.png. In this case the database would contain `image/avatar-32425.png`
- One modern PNG would have a URL like https://modern-url/avatar/avatar-32425.png. In this case the database would contain `avatar/avatar-32425.png`

### Your task

You need to write a program that moves all images from the `legacy-s3` to the `production-s3` and updates their paths in the database. To clarify, the program will make sure that all objects in the legacy bucket/path are correctly moved to the new one. This means, that at the end of the execution, the database will also contain only paths with the modern prefix.

### Resources

Your program will interact, obviously, with some services. A service that will give you the functionality so store files in a S3 fashion and a database in which the references to all the S3 files is written.

- `production-db` PostgreSQL database
- `production-s3` AWS S3 bucket
- `legacy-s3` AWS S3 bucket

You must use a service compatible with the S3 API for basic read/write operations. You can either use S3 from AWS, or Google Cloud Store which is compatible with the basic AWS S3 API capabilities or user a local emulation with one of the free services out there, like for example Minio. As long as your program uses a compatible S3 API, it's up to you what to use.

If you want, you can use the provided database schema and seeding script (python 3, requires `psycopg2-binary` and `boto3`) that will generate both the database rows and the S3 objects for the legacy assets, in any existing PostgreSQL database and S3 bucket, so you can focus on your program instead of in setting everything up.

### We expect

- The program to be written in a real programming language like Python, Ruby, Go, Elixir, etc… Please, avoid shell scripting or similar. We don't expect a code masterpiece but we expect clean code that is easy to understand and modify without major refactors.
- The program to run in a POSIX-compliant environment.
- To be able to test and run the program with minimal setup/configuration.
- Documentation in the form of comments in the code and a README file explaining the function and how to run your solution.
- The program to handle most common problems. It should not crash if something unexpected happens and it should be able to resume if it stops at the middle.
- The program to be fast and efficient; assume the buckets store hundreds of millions of images, and we want to move them in the shortest time possible. Please provide comments about performance and scalability when you send the challenge back to us.
- The program to be able to run in a production environment. Meaning that the information located in the database must always point to an existing PNG in S3 and not cause any service disruption.
- You to write a few lines explaining your development setup (how did you create or emulate the resources).
- The code of the challenge written in a Git repository, zipped and uploaded to Ashby. Try to not write the entire program in one commit and version it as much as you can. For us, understanding your progress is valuable.
- A description of your deployment plan to production. We don't need to see any IaC, we only want a written explanation.

### Extra ball

You can use any S3 and database user you want, but it would great that you describe what privileges the PostgreSQL user and the S3 user needs to have to be able to perform the operations needed by your program.
