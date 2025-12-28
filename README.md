# Games-ETL-Pipeline

# Project plan

## Objective

The objective of this project is to provide analytical datasets derived from the RAWG Video Game Database API. 

## Consumers

The users of my datasets are game enthusiasts and data analysts. Hopefully one day I can make the data easily accessible and interactive.


## Questions

> - What game tags are the most popular?
> - What tags show up the most within which genre?
> - Which platforms have the higest rated games?
> - Which decade had the highest rating games?
> - How many games are in each genre?

## Source datasets

| Source name | Source type | Source documentation | Extract Type | Load Type
| - | - | - | - | - |
| RAWG Video Games Databaes API | REST API | https://rawg.io/apidocs | NA | NA |
| RAWG - Games | REST API | https://api.rawg.io/docs/#tag/games | Full Extract | Upsert |
| RAWG - Genres | REST API | https://api.rawg.io/docs/#tag/genres | Full Extract | Upsert |
| RAWG - Platforms | REST API | https://api.rawg.io/docs/#tag/platforms | Full Extract | Upsert |
| RAWG - Tags | REST API | https://api.rawg.io/docs/#tag/tags | Full Extract | Upsert |

The RAWG Video Games Databaes API does not update on a fixed schedule but updates are frequent due to constantly adding new releases and updating existing info about games already in the database.

## Solution architecture

DIAGRAM TO BE POSTED

We recommend using a diagramming tool like [draw.io](https://draw.io/) to create your architecture diagram.

Here is a sample solution architecture diagram:

![images/sample-solution-architecture-diagram.png](images/sample-solution-architecture-diagram.png)

## Preset

Insert Images of Preset Dashboard