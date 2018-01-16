import traceback
import logging

from pymongo import MongoClient
from bson.objectid import ObjectId
from neo4j.v1 import GraphDatabase, basic_auth, CypherError

from enrichment.enrichment_service_config import EnrichmentServiceConfig


class DBWrapper(object):

    ACURERATE_DB_NAME = "acurerate"
    PEOPLE_COLLECTION_NAME = "people"
    COMPANY_COLLECTION_NAME = "company"
    UNVERIFIED_COMPANIES_COLLECTION_NAME = "unverified_companies"

    DB_HOST = "localhost"
    DB_PORT = 27017

    acurerate_db = None

    people_collection = None
    company_collection = None
    unverified_companies_collection = None

    logger = None

    def __init__(self):
        pass

    @staticmethod
    def db():
        return DBWrapper.acurerate_db

    # @staticmethod
    # def set_logger(logger):
    #     DBWrapper.logger = logger

    @staticmethod
    def connect():

        # Check if we are already connected
        if DBWrapper.acurerate_db is not None:
            return

        DBWrapper.logger = logging.getLogger(EnrichmentServiceConfig.LOGGER_NAME)
        if not DBWrapper.logger:
            print('*** No logger set. Aborting connect action. ***')
            return


        # TODO: read from configuration
        acurerate_db_name = DBWrapper.ACURERATE_DB_NAME
        people_collection_name = DBWrapper.PEOPLE_COLLECTION_NAME
        company_collection_name = DBWrapper.COMPANY_COLLECTION_NAME
        unverified_companies_collection_name = DBWrapper.UNVERIFIED_COMPANIES_COLLECTION_NAME

        # Connect to MongoDB
        mongo_client = MongoClient(DBWrapper.DB_HOST, DBWrapper.DB_PORT)
        available_dbs = mongo_client.database_names()
        if acurerate_db_name in available_dbs:
            DBWrapper.acurerate_db = mongo_client[acurerate_db_name]
            available_collections = DBWrapper.acurerate_db.collection_names()
            if people_collection_name in available_collections:
                DBWrapper.people_collection = DBWrapper.acurerate_db[people_collection_name]
            else:
                DBWrapper.logger.error("Cannot locate people collection")

            if company_collection_name in available_collections:
                DBWrapper.company_collection = DBWrapper.acurerate_db[company_collection_name]
            else:
                DBWrapper.logger.error("Cannot locate company collection")

            if unverified_companies_collection_name in available_collections:
                DBWrapper.unverified_companies_collection = DBWrapper.acurerate_db[unverified_companies_collection_name]
            else:
                DBWrapper.logger.error("Cannot locate unrecognized_companies collection")
        else:
            DBWrapper.logger.error("Unable to locate acurerate DB in Mongo")

        DBWrapper.logger.info('Connected to MongoDB database!')

    @staticmethod
    def get_entities(collection_name, query, single_result=False):
        if collection_name is None or query is None:
            return None
        collection = DBWrapper.acurerate_db[collection_name]
        if single_result:
            cursor = collection.find_one(query)
        else:
            cursor = collection.find(query)
        return cursor

    @staticmethod
    def set_entity(collection_name, entity):
        # if entity is None or entity.aid is None:
        #     raise Exception('DBWrapper.set_entity: Failed to modify entity - None or no AID.')
        collection = DBWrapper.acurerate_db[collection_name]
        if hasattr(entity, '_aid'):
            update_result = collection.update({'_id': ObjectId(entity.aid)}, entity.__dict__, upsert=False)
        else:
            result = collection.insert_one(entity.__dict__)
            entity.aid = str(result.inserted_id)
            collection.update_one({'_id': ObjectId(entity.aid)},
                                  {"$set": {"_aid": entity.aid}})
        pass

    @staticmethod
    def get_persons(query, single_result=False):
        if query is None:
            return None
        if single_result:
            cursor = DBWrapper.people_collection.find_one(query)
        else:
            cursor = DBWrapper.people_collection.find(query)
        return cursor

    @staticmethod
    def store_persons(query, person):
        if query is None or person is None:
            return None
        update_result = DBWrapper.people_collection.update(query, person.__dict__, upsert=True)
        pass


    @staticmethod
    def get_companies(query, single_result=False):
        if query is None:
            return None
        if single_result:
            cursor = DBWrapper.company_collection.find_one(query)
        else:
            cursor = DBWrapper.company_collection.find(query)
        return cursor

    @staticmethod
    def get_collections():
        DBWrapper.acurerate_db.collection_names()

    @staticmethod
    def replace_person(person):
        query = {"_id": ObjectId(str(person.aid))}
        result = DBWrapper.people_collection.replace_one(query, person.__dict__)
        if result.modified_count == 0:
            raise Exception('DBWrapper.replace_person: Failed to modify person with aid = %s' % str(person._aid))
        return result.modified_count

    @staticmethod
    def delete_person(person):
        # Remove by _id (in case the person does not have _aid set)
        query = {"_id": ObjectId(str(person.aid))}
        # Delete if found
        result = DBWrapper.people_collection.delete_one(query)
        if result.deleted_count == 0:
            raise Exception('DBWrapper.delete_person: Failed to delete person with aid = %s' % str(person._aid))
        return result.deleted_count

    @staticmethod
    def get_potential_cliques(pivot_aid):
        cliques_map = {}
        cliques_to_join = []
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "0internet1"))
        session = driver.session()

        # Check if there are 2-Cliques who pivot can join (not marked yet as Clique
        cql_r_str = "MATCH (p1:Person)-[r1]->(p2:Person)-[r2]->(p1:Person), " + \
                    "      (p0:Person)-[r3]->(p5:Person)-[r4]->(p0:Person), " + \
                    "      (p0:Person)-[r5]->(p6:Person)-[r6]->(p0:Person) " + \
                    "WHERE p0.aid = '%s' AND id(p1) = id(p5) AND id(p2) = id(p6) " % pivot_aid + \
                    "RETURN p1.name, p2.name"
        statement_res = session.run(cql_r_str)
        potential_pairs = set()
        for x in statement_res:
            name1 = x['p1.name']
            name2 = x['p2.name']
            if (name1, name2) not in potential_pairs and (name2, name1) not in potential_pairs:
                potential_pairs.add((name1, name2))

        # Check if there are pivot's contacts pointing back at him and already part of cliques
        cql_r_str = "MATCH (p0:Person)-[rp1]->(p1:Person)-[rp2]->(p0:Person), (p1:Person)-[rc1]->(cl:Clique)" + \
                    "WHERE p0.aid = '%s' " % pivot_aid + \
                    "RETURN p1.name, id(cl)"
        statement_res = session.run(cql_r_str)
        # Go over results and map them to cliques
        for x in statement_res:
            name = x['p1.name']
            clique_id = x['id(cl)']
            if clique_id not in cliques_map:
                cliques_map[clique_id] = set()
            cliques_map[clique_id].add(name)

        # Go over cliques and make sure all those found are part of it
        for clique_id, clique_set in cliques_map.items():
            pp = DBWrapper.people_in_clique(clique_id)  # 96194
            if len(pp) == len(clique_set):
                cliques_to_join.append(clique_id)

        # Go over the pairs and check if they were already part of bigger cliques found
        # TODO: complete implementation here.... My brain is melting already, Arggggg... :-)
        # for x, y in potential_pairs:
        #     for clique_id, clique_set in cliques_map.items():
        #         if (x, y) in clique_set...

        session.close()
        return cliques_to_join

    @staticmethod
    def people_in_clique(clique_id):
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "0internet1"))
        session = driver.session()
        people = set()
        cql_str = "MATCH (p:Person)-[r:IN_CLIQUE]->(cl:Clique) " + \
                  "WHERE id(cl) = %s " % clique_id + \
                  "RETURN p"  # TODO: p.aid ...
        statement_res = session.run(cql_str)
        for x in statement_res:
            the_id = x['p']['aid']
            # the_name = x['p'].name
            people.add(the_id)

        session.close()
        return list(people)

    @staticmethod
    def person_cliques(aid):
        cliques = []
        # Check in Neo4J
        return cliques

    @staticmethod
    def update_cliques(pivot_aid):
        pass

    @staticmethod
    def get_aggregation_2(match, fields, unwind=None):
        pass

    def get_aggregation(query):
        # @@@
        pipeline = [
            #{"$match": {"deduced.last_name": "Gilon"}
            {"$unwind": "$deduced.emails"},
            {"$group": {"_id": query, "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}}
        ]
        cursor = DBWrapper.people_collection.aggregate(pipeline)
        for c in cursor:
            print(c)
        pass

    @staticmethod
    def maintain_mongo():
        #q = {"deduced.email": "tzahiw@hotmail.com"}
        q = {}
        cursor = DBWrapper.get_persons(q)
        for r in cursor:
            changed = False
            try:
                for provider_name in r['data_sources'].keys():
                    for ds in r['data_sources'][provider_name]:
                        if 'enrich_key' not in ds:
                            if 'attribution_id' in ds:
                                ds['enrich_key'] = ds['attribution_id']
                                changed = True
                            elif 'search_key' in ds:
                                ds['enrich_key'] = ds['search_key']
                                changed = True
                            elif provider_name == 'CrunchBaseBot' and 'cb_permalink' in ds:
                                ds['enrich_key'] = ds['cb_permalink']
                                changed = True
                            else:
                                pass
                if changed:
                    DBWrapper.people_collection.save(r)

            except Exception as e:
                print("Problem with %s" % r['deduced']['name'])
                traceback.print_exc()

    @staticmethod
    def maintain_mongo4():
        counter = 0
        try:
            #q = {"deduced.name": "Optimove"}
            q = {}
            cursor = DBWrapper.get_companies(q)
            for r in cursor:
                changed = False
                try:
                    for provider_name in r['data_sources'].keys():
                        if isinstance(r['data_sources'][provider_name], (list, tuple)):
                            continue
                        changed = True
                        if 'CrunchBaseBot' in provider_name:
                            r['data_sources'][provider_name]['enrich_key'] = r['data_sources'][provider_name]['cb_permalink']
                        elif 'FullContact' in provider_name:
                            if 'search_key' in r['data_sources'][provider_name]:
                                r['data_sources'][provider_name]['enrich_key'] = r['data_sources'][provider_name]['search_key']
                            else:
                                r['data_sources'][provider_name]['enrich_key'] = 'nokey'
                        else:
                            r['data_sources'][provider_name]['enrich_key'] = 'nokey'
                        r['data_sources'][provider_name] = [r['data_sources'][provider_name]]
                    if changed:
                        DBWrapper.company_collection.save(r)
                except Exception as e:
                    print("Problem with %s" % r['deduced']['name'])
                    traceback.print_exc()

                counter += 1
                if counter % 1000 == 0:
                    print('Done fixing %d rows...', counter)

            pass
        except Exception as e:
            traceback.print_exc()
        pass

    @staticmethod
    def maintain_mongo3():
        try:
            #q = {"deduced.email": "boaz.cha@gmail.com"}
            q = {}
            cursor = DBWrapper.get_persons(q)
            for r in cursor:
                changed = False
                google_contacts = []
                linkedin_contacts = []
                to_delete = []
                for provider_name in r['data_sources'].keys():
                    if isinstance(r['data_sources'][provider_name], (list, tuple)):
                        continue
                    changed = True
                    if 'GoogleContacts' not in provider_name and 'LinkedInContacts' not in provider_name:
                        r['data_sources'][provider_name] = [r['data_sources'][provider_name]]
                    else:
                        if provider_name == 'GoogleContacts-578f49f348d72719070ea206':
                            google_contacts.append(r['data_sources']['GoogleContacts-578f49f348d72719070ea206'])
                            to_delete.append('GoogleContacts-578f49f348d72719070ea206')
                        if provider_name == 'GoogleContacts-578b58cb48d72719070d6528':
                            google_contacts.append(r['data_sources']['GoogleContacts-578b58cb48d72719070d6528'])
                            to_delete.append('GoogleContacts-578b58cb48d72719070d6528')
                        if provider_name == 'LinkedInContacts-578f49f348d72719070ea206':
                            linkedin_contacts.append(r['data_sources']['LinkedInContacts-578f49f348d72719070ea206'])
                            to_delete.append('LinkedInContacts-578f49f348d72719070ea206')
                        if provider_name == 'LinkedInContacts-578b58cb48d72719070d6528':
                            linkedin_contacts.append(r['data_sources']['LinkedInContacts-578b58cb48d72719070d6528'])
                            to_delete.append('LinkedInContacts-578b58cb48d72719070d6528')
                if len(google_contacts) > 0:
                    r['data_sources']['GoogleContacts'] = google_contacts
                if len(linkedin_contacts) > 0:
                    r['data_sources']['LinkedInContacts'] = linkedin_contacts
                if len(google_contacts) > 1:
                    pass
                if len(linkedin_contacts) > 1:
                    pass
                for k in to_delete:
                    del r['data_sources'][k]
                if changed:
                    DBWrapper.people_collection.save(r)
                else:
                    print("Not sure why there's no change here - check it")
                pass
        except Exception as e:
            print(e)
        pass

    @staticmethod
    def maintain_mongo2():
        coll = DBWrapper.people_collection
        try:
            bulkop = coll.initialize_ordered_bulk_op();

            find_query = {"data_sources.LinkedInContacts": {"$exists": True}}
            #find_query = {"deduced.email": "boaz.cha@gmail.com"}

            if False:
                retval = bulkop.find(find_query).update(
                    {"$set": {"data_sources.LinkedInContacts.attribution_name": "Dror Garti"}})
                retval = bulkop.find(find_query).update(
                    {"$set": {"data_sources.LinkedInContacts.attribution_id": "578f49f348d72719070ea206"}})
            else:
                retval = bulkop.find(find_query).update(
                    {"$rename": {"data_sources.LinkedInContacts": "data_sources.LinkedInContacts-578f49f348d72719070ea206"}})

            bulkop.execute();
        except Exception as e:
            print(e)
        pass

    @staticmethod
    def remove_fields(query, field_name):
        coll = DBWrapper.people_collection
        retval = coll.update(query, {"$unset": {field_name: 1}}, upsert=False, multi=True)
        pass
