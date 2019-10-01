# Dfixer

Create an application with two modes.

## Dfixer standard mode

A tool to fix vivo data problems that tend to only need aplied once. These tend to take a long time and will be done in parts.

### Input

A list of uri or a query that returns uri of all subjects the oneshot needs to run on.

### Flags

- "--cleaner" \- takes a query or a uri(s)and runs specific cleaner on it.

- "--query" \- takes in query to run (must be nammed after the cleaner to know what cleaner to use).

- "--multishot" \- runs all possible oneshots for given uri(s).

## Multishot mode

Multishots run all appliable oneshot fixes on one subject.

## Vocab

- Dfixer \- will take a query and fix all problems with resulting uri for a specific OneShot.

- OneShot \- file that fixes on specific data problem.

- MultiShot \- runs multiple oneshots on a uri.

## Making oneshots

Use the abstract base class. Must have both get_sub_trips and get_add_trips. It is sugested to use subject types when apply and repeate if oneshot will run repeatedly (mostlikely through BASH)

#### Notes

have oneshots double check if uri really needs the oneshot.
