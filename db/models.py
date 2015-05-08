#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
from peewee import *
import os, sys

sys.path.append(os.path.realpath(os.path.curdir))

db = SqliteDatabase('ksdata.db')


class BaseModel(Model):
    class Meta:
        database = db

class Category(BaseModel):
    ks_id = IntegerField(unique=True,default=None,null=False)
    name = CharField(max_length=20,index=True)
    parent = IntegerField(null=True)
    position = IntegerField()
    slug = CharField(max_length=28,index=True)
    url = TextField()

    @classmethod
    def _create(cls,proj_cat):
        try:
            cat = Category.get(Category.slug==proj_cat["slug"])
#            print "Cat something: %s" % str(dir(existing))
#            cat = existing[0]
#            print "Cat hit: %s " % proj_cat["slug"]
        except Exception as e:
            print "Cat miss: (%s) %s " % (str(e),proj_cat["slug"])
            with db.transaction():
                cat = Category(ks_id=proj_cat["id"])
                cat.name = proj_cat["name"]
                if "parent_id" in proj_cat:
                    cat.parent = proj_cat["parent_id"]
                cat.position = proj_cat["position"]
                cat.slug = proj_cat["slug"]
                cat.url = proj_cat["urls"]["web"]["discover"]
                cat.save()
        return cat

class Location(BaseModel):
    ks_id = IntegerField(unique=True,default=None,null=False)
    country = CharField(max_length=2)
    displayable_name = CharField(max_length=50)
    name = CharField(max_length=40,index=True)
    short_name = CharField(max_length=40)
    slug = CharField(max_length=60)
    state = CharField(max_length=50,index=True)
    #loc_type = CharField(max_length=18)
    url_projects_nearby = TextField()
    url_discover = TextField()
    url_location = TextField()

    @classmethod
    def _create(cls,proj_loc):
        try:
            loc = Location.get(Location.url_location==proj_loc["urls"]["web"]["location"])
#            print "Loc something: %s" % str(dir(existing))
#            L = existing[0]
#            print "Loc hit: %s " % proj_loc["urls"]["web"]["location"]
        except Exception as e:
            print "Loc miss: (%s) %s " % (str(e),proj_loc["urls"]["web"]["location"])
            with db.transaction():
                loc = Location(ks_id=proj_loc["id"])
                loc.country = proj_loc["country"]
                loc.displayable_name = proj_loc["displayable_name"]
                loc.name = proj_loc["name"]
                loc.short_name = proj_loc["short_name"]
                loc.slug = proj_loc["slug"]
                loc.state = proj_loc["state"]
                loc.type = proj_loc["type"]
                loc.url_projects_nearby = proj_loc["urls"]["api"]["nearby_projects"]
                loc.url_discover = proj_loc["urls"]["web"]["discover"]
                loc.url_location = proj_loc["urls"]["web"]["location"]
                loc.save()
        return loc

class HttpCache(BaseModel):
    url = CharField(max_length=255,unique=True,null=False)
    content = BlobField()

    @classmethod
    def _create(cls,new_url,blob):
        entry = HttpCache(url=new_url,content=blob)
        entry.save()
        return entry

class Project(BaseModel):
    ks_id = IntegerField(unique=True,default=None,null=False)
    backers_count = IntegerField(default=0)
    name = CharField(max_length=100)
    slug = CharField(max_length=130)
    blurb = CharField(max_length=150)
    category = ForeignKeyField(Category,index=True)
    location = ForeignKeyField(Location,index=True)
    created = DateTimeField(null=False,index=True)
    deadline = DateTimeField(null=False,index=True)
    launched = DateTimeField(null=False,index=True)
    currency = CharField(max_length=3)
    goal = FloatField()
    pledged = FloatField()
    state = CharField(max_length=10)
    url_project = TextField()
    url_rewards = TextField()

    @classmethod
    def _create(cls,project):
        try:
            proj=Project.get(Project.url_project==project["urls"]["web"]["project"])
            #print "Proj something: %s" % str(proj).encode('ascii','replace')
            #print "Proj hit: %s " % project["urls"]["web"]["project"]
        except Exception as e:
            print "Proj miss: (%s) %s " % (str(e),project["urls"]["web"]["project"])
            with db.transaction():
                proj = Project(ks_id=project["id"])
                proj.name = project["name"]
                proj.slug = project["slug"]
                proj.blurb = project["blurb"]
                proj.country = project["country"]
                proj.created = project["created_at"]
                proj.deadline = project["deadline"]
                proj.launched = project["launched_at"]
                proj.currency = project["currency"]
                proj.goal = project["goal"]
                proj.pledged = project["pledged"]
                proj.state = project["state"]
                proj.url_project = project["urls"]["web"]["project"]
                proj.url_rewards = project["urls"]["web"]["rewards"]
                proj.category = Category._create(project["category"])
                proj.location = Location._create(project["location"])
                proj.save()
        return proj

    def __str__(self):
        return self.name

db.connect()
db.create_tables([Category,Location,Project,HttpCache], safe=True)
#print dir(db)
#db.drop_tables([Category,Location,Project],safe=False)
#db.create_tables([Category,Location,Project], safe=False)

  
"""
{
    "backers_count": {
        "int": 105857
    }, 
    "blurb": {
        "unicode": 150
    }, 
    "category": {
        "id": {
            "int": 362
        }, 
        "name": {
            "unicode": 18
        }, 
        "parent_id": {
            "int": 26
        }, 
        "position": {
            "int": 19
        }, 
        "slug": {
            "unicode": 28
        }, 
        "urls": {
            "web": {
                "discover": {
                    "unicode": 81
                }
            }
        }
    }, 
    "country": {
        "unicode": 2
    }, 
    "created_at": {
        "int": 1417530582
    }, 
    "creator": {
        "avatar": {
            "medium": {
                "unicode": 286
            }, 
            "small": {
                "unicode": 285
            }, 
            "thumb": {
                "unicode": 285
            }
        }, 
        "id": {
            "int": 2147460483
        }, 
        "name": {
            "unicode": 50
        }, 
        "slug": {
            "unicode": 20
        }, 
        "urls": {
            "api": {
                "user": {
                    "unicode": 109
                }
            }, 
            "web": {
                "user": {
                    "unicode": 56
                }
            }
        }
    }, 
    "currency": {
        "unicode": 3
    }, 
    "currency_symbol": {
        "unicode": 2
    }, 
    "currency_trailing_code": {
        "bool": true
    }, 
    "deadline": {
        "int": 1422727710
    }, 
    "disable_communication": {
        "bool": true
    }, 
    "goal": {
        "float": 21474836.47, 
        "int": 100000000
    }, 
    "id": {
        "int": 2147472329
    }, 
    "launched_at": {
        "int": 1417543710
    }, 
    "location": {
        "country": {
            "unicode": 2
        }, 
        "displayable_name": {
            "unicode": 50
        }, 
        "id": {
            "int": 99999999
        }, 
        "is_root": {
            "bool": false
        }, 
        "name": {
            "unicode": 39
        }, 
        "short_name": {
            "unicode": 46
        }, 
        "slug": {
            "unicode": 56
        }, 
        "state": {
            "unicode": 49
        }, 
        "type": {
            "unicode": 13
        }, 
        "urls": {
            "api": {
                "nearby_projects": {
                    "unicode": 117
                }
            }, 
            "web": {
                "discover": {
                    "unicode": 100
                }, 
                "location": {
                    "unicode": 94
                }
            }
        }
    }, 
    "name": {
        "unicode": 96
    }, 
    "photo": {
        "1024x768": {
            "unicode": 75
        }, 
        "1536x1152": {
            "unicode": 76
        }, 
        "ed": {
            "unicode": 69
        }, 
        "full": {
            "unicode": 71
        }, 
        "little": {
            "unicode": 73
        }, 
        "med": {
            "unicode": 70
        }, 
        "small": {
            "unicode": 72
        }, 
        "thumb": {
            "unicode": 72
        }
    }, 
    "pledged": {
        "float": 13285226.36, 
        "int": 3390551
    }, 
    "slug": {
        "unicode": 121
    }, 
    "state": {
        "unicode": 10
    }, 
    "state_changed_at": {
        "int": 1417543710
    }, 
    "urls": {
        "web": {
            "project": {
                "unicode": 181
            }, 
            "rewards": {
                "unicode": 176
            }
        }
    }
}
"""
