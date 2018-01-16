import logging
from utils.acurerate_utils import AcureRateUtils
from enrichment.enrichment_service_config import EnrichmentServiceConfig

from entities.acurerate_attributes import P, C, G
from entities. acurerate_job import AcureRateJob

from neo4j.v1 import GraphDatabase, basic_auth, CypherError


class NeoWrapper(object):

    DB_HOST = "bolt://localhost"
    DB_PORT = 7687
    USERNAME = "neo4j"
    PASSWORD = "0internet1"

    driver = None
    logger = None

    def __init__(self):
        pass

    # @staticmethod
    # def set_logger(logger):
    #     NeoWrapper.logger = logger

    @staticmethod
    def connect():
        if NeoWrapper.driver is not None:
            return

        NeoWrapper.logger = logging.getLogger(EnrichmentServiceConfig.LOGGER_NAME)
        if not NeoWrapper.logger:
            print('*** No logger set. Aborting connect action. ***')
            return

        url = '%s:%s' % (NeoWrapper.DB_HOST, NeoWrapper.DB_PORT)
        NeoWrapper.driver = GraphDatabase.driver(url, auth=basic_auth(NeoWrapper.USERNAME, NeoWrapper.PASSWORD))
        NeoWrapper.logger.info('Connected to Neo4J database!')

    @staticmethod
    def set_person(person):
        NeoWrapper.set_person_properties(person)
        NeoWrapper.set_person_relationships(person)
        pass

    @staticmethod
    def set_company(company):
        NeoWrapper.set_company_properties(company)
        NeoWrapper.set_company_relationships(company)
        pass

    @staticmethod
    def get_property_by_type(property_name, property_value, kv_separator):
        if property_name is None or property_value is None:
            return None
        if isinstance(property_value, str):
            the_string = property_value
            if "'" in the_string:
                the_string = the_string.replace("'", "")
            s = "%s%s'%s'" % (property_name, kv_separator, the_string)
        elif isinstance(property_value, bool):
            s = "%s%s%s" % (property_name, kv_separator, str(property_value).lower())
        elif isinstance(property_value, int):
            s = "%s%s%s" % (property_name, kv_separator, int(property_value))
        elif isinstance(property_value, list):
            clean_list = [item for item in property_value if item is not None]
            s = "%s%s%s" % (property_name, kv_separator, clean_list)
        elif isinstance(property_value, set):
            clean_list = [item for item in property_value if item is not None]
            s = "%s%s%s" % (property_name, kv_separator, clean_list)
        else:
            s = "something else"
        return s

    @staticmethod
    def _calc_strength(x):
        strength = 0
        for i in range(1, 5):
            k = 'r%d.strength' % i
            if k in x and x[k]:
                strength += x[k]
        return strength

    @staticmethod
    def get_paths_to_person(source_id, target_id):
        paths = []

        # Get session for query
        session = NeoWrapper.driver.session()

        # Run query to locate possible direct connection to target
        # Source:Person -[CONTACT]-> Target:Person
        cql_r_str = "MATCH (s:Person)-[r1]->(t:Person) " + \
                    "WHERE s.aid = '%s' AND t.aid = '%s' " % (source_id, target_id) + \
                    "RETURN s.name, type(r1), r1.strength, t.name"
        statement_res = session.run(cql_r_str)

        # Iterate over results and construct paths from segments
        for x in statement_res:
            segments = [
                [x['s.name'], x['type(r1)'], x['t.name']]
            ]
            paths.append((NeoWrapper._calc_strength(x), segments))

        # Run query to locate possible connection to target via same company
        # Source:Person -[]-> Company <-[]- Target:Person
        cql_r_str = "MATCH (s:Person)-[r1]->(c:Company)<-[r2]-(t:Person) " + \
                    "WHERE s.aid = '%s' AND t.aid = '%s' " % (source_id, target_id) + \
                    "RETURN s.name, type(r1), r1.strength, c.name, type(r2), r2.strength, t.name"
        statement_res = session.run(cql_r_str)

        # Iterate over results and construct paths from segments
        for x in statement_res:
            segments = [
                [x['s.name'], x['type(r1)'], x['c.name']],
                [x['t.name'], x['type(r2)'], x['c.name']]
            ]
            paths.append((NeoWrapper._calc_strength(x), segments))

        # Run query to locate the referrals through direct contacts
        # Source:Person -[CONTACT]-> Referral:Person -[CONTACT]-> Target:Person -[R]-> Company
        cql_r_str = "MATCH (s:Person)-[r1]->(r:Person), " + \
                    "      (r:Person)-[r2]->(t:Person) " + \
                    "WHERE s.aid = '%s' AND t.aid = '%s' " % (source_id, target_id) + \
                    "RETURN s.name, type(r1), r1.strength, r.name, type(r2), r2.strength, t.name"
        statement_res = session.run(cql_r_str)

        # Iterate over results and construct paths from segments
        for x in statement_res:
            segments = [
                [x['s.name'], x['type(r1)'], x['r.name']],
                [x['r.name'], x['type(r2)'], x['t.name']]
            ]
            paths.append((NeoWrapper._calc_strength(x), segments))

        session.close()

        # (a) Sort by score and (b) Strip the score
        sorted_paths = sorted(paths, key=lambda tup: tup[0], reverse=True)
        final_paths = [path for score, path in sorted_paths]
        return final_paths

    @staticmethod
    def get_paths_to_company(source_id, target_id, seniority=None, area=None):
        paths = []

        # Get session for query
        session = NeoWrapper.driver.session()

        # Run query to locate the referrals through direct contacts
        # Source:Person -[CONTACT]-> Referral:Person -[CONTACT]-> Target:Person -[R]-> Company
        condition_line_1 = ' AND "%s" in r3.jobs_seniorites' % seniority if seniority else ''
        condition_line_2 = ' AND "%s" in r3.jobs_areas' % area if area else ''
        cql_r_str = "MATCH (s:Person)-[r1]->(r:Person), " + \
                    "      (r:Person)-[r2]->(t:Person), " + \
                    "      (t:Person)-[r3]->(c:Company) " + \
                    "WHERE s.aid = '%s' AND c.aid = '%s' " % (source_id, target_id) + \
                    "%s%s" % (condition_line_1, condition_line_2) + \
                    "RETURN s.name, type(r1), r1.strength, r.name, type(r2), r2.strength, t.name, type(r3), r3.strength, c.name"
        statement_res = session.run(cql_r_str)

        # Iterate over results and construct paths from segments
        for x in statement_res:
            segments = [
                [x['s.name'], x['type(r1)'], x['r.name']],
                [x['r.name'], x['type(r2)'], x['t.name']],
                [x['t.name'], x['type(r3)'], x['c.name']]
            ]
            paths.append((NeoWrapper._calc_strength(x), segments))

        # Run query to locate paths with referrals that worked with another person in the same company
        # Source:Person -[CONTACT]-> Referral:Person -[R1]-> Company-1 <- Target:Person <-[R2] Company
        cql_r_str = "MATCH (fp:Person)-[r1:CONTACT]-(rp:Person), " + \
                    "      (rp:Person)-[r2:EMPLOYEE_OF]->(md:Company), " + \
                    "      (tp:Person)-[r3]-(md:Company), " + \
                    "      (tp:Person)-[r4]-(tc:Company) " + \
                    "WHERE fp.aid = '%s' AND tc.aid = '%s' " % (source_id, target_id) + \
                    "RETURN fp.name, type(r1), r1.strength, rp.name, type(r2), r2.strength, md.name, tp.name, type(r3), r3.strength, type(r4), r4.strength, tc.name"
        statement_res = session.run(cql_r_str)

        # Iterate over results and construct paths from segments
        for x in statement_res:
            segments = [
                [x['fp.name'], x['type(r1)'], x['rp.name']],
                [x['rp.name'], x['type(r2)'], x['md.name']],
                [x['tp.name'], x['type(r3)'], x['md.name']],
                [x['tp.name'], x['type(r4)'], x['tc.name']]
            ]
            scored_segment = (NeoWrapper._calc_strength(x), segments)
            paths.append(scored_segment)

        # Source:Person -[R1]-> Company <-[R2]- Referral:Person -[R1]-> Company-1 <- Target:Person <-[R2] Company

        session.close()

        # (a) Sort by score and (b) Strip the score
        sorted_paths = sorted(paths, key=lambda tup: tup[0], reverse=True)
        final_paths = [path for score, path in sorted_paths]
        return final_paths

    @staticmethod
    def set_person_properties(person):

        # Sanity check to make sure there's aid
        if not person.aid:
            NeoWrapper.logger.warning('No aid for person %s. Not migrated.' % person)
            return

        session = NeoWrapper.driver.session()

        try:
            properties = person.get_properties()
            labels = person.get_labels()

            # Add 'name' for the Neo4J Browser :-)
            if P.FULL_NAME in person.deduced:
                properties['name'] = person.deduced[P.FULL_NAME].replace("'", "")

            # Add 'aid' for the Neo4J to be able to relate back to Mongo
            properties['aid'] = person.aid

            # Set the node with properties and labels
            # TODO: if node exists, we may want to remove all previous labels, because query will not remove them
            cql_str = 'MERGE (n:Person {aid: "%s"}) ' % person.aid + \
                      'SET n = {props}, n:%s' % ':'.join(labels)
            statement_res = session.run(cql_str, {'props': properties})
            NeoWrapper.logger.info('Migrated person %s successfully! Details: %s' %
                                   (person.deduced[P.FULL_NAME],
                                    AcureRateUtils.obj2string(statement_res.consume().counters)))
        except Exception as e:
            NeoWrapper.logger.error('Migration of person node %s failed. Exception raised: %s' %
                                    (person.deduced[P.FULL_NAME] if person else 'none', e))
        finally:
            session.close()
        pass

    @staticmethod
    def set_person_relationships(pivot_person):
        from store.store import Store

        session = NeoWrapper.driver.session()

        relations = pivot_person.get_relations()
        try:
            for source_aid, relation_type, target_aid, relation_properties in relations:
                cql_r_str = "MATCH (source),(target) " + \
                            "WHERE source.aid = '%s' AND target.aid = '%s' " % (source_aid, target_aid) + \
                            "MERGE (source)-[r:%s{%s}]->(target)" % (relation_type, relation_properties)
                statement_res = session.run(cql_r_str)
                # TODO: inspect statement_res and add it to log
                NeoWrapper.logger.info('Created person %s relation %s-[%s]-%s succesfully! Details: %s',
                                       pivot_person.deduced[P.FULL_NAME], source_aid, relation_type, target_aid,
                                       AcureRateUtils.obj2string(statement_res.consume().counters))
            pass
        except Exception as e:
            NeoWrapper.logger.error('Migrated relations of person %s failed. Exception raised: %s' %
                                    (pivot_person.deduced[P.FULL_NAME] if pivot_person else 'none', e))
        finally:
            session.close()
        pass

    @staticmethod
    def set_company_properties(company):

        # Sanity check to make sure there's aid
        if not company.aid:
            NeoWrapper.logger.warning('No aid for company %s. Not migrated.' % company)
            return

        # TODO: TEMP TEMP!!  We ignore all those who were initially inserted only by CB Excel
        if len(company.data_sources) == 1 and 'CrunchBase2014' in company.data_sources:
            return

        session = NeoWrapper.driver.session()

        try:
            properties = company.get_properties()
            labels = company.get_labels()

            # Add 'aid' for the Neo4J to be able to relate back to Mongo
            properties['aid'] = company.aid

            # Set the node with properties and labels
            # TODO: if node exists, we may want to remove all previous labels, because query will not remove them
            cql_str = 'MERGE (n:Company {aid: "%s"}) ' % company.aid + \
                      'SET n = {props}, n:%s' % ':'.join(labels)
            statement_res = session.run(cql_str, {'props': properties})
            NeoWrapper.logger.info('Migrated company %s successfully! Details: %s' %
                                   (company.deduced[C.NAME],
                                    AcureRateUtils.obj2string(statement_res.consume().counters)))
        except Exception as e:
            NeoWrapper.logger.error('Migration of company node %s failed. Exception raised: %s' %
                                    (company.deduced[C.NAME] if company else 'none', e))
        finally:
            session.close()
        pass

    @staticmethod
    def set_company_relationships(company):
        from store.store import Store

        session = NeoWrapper.driver.session()
        try:
            # TODO: TEMP TEMP!!  We ignore all those who were initially inserted only by CB Excel
            if len(company.data_sources) == 1 and 'CrunchBase2014' in company.data_sources:
                return

            relations = company.get_relations()

            NeoWrapper.logger.info('Attempting to create relations for company %s', company.deduced[C.NAME])

            # Go over relations and create them in Neo4J
            for source_aid, relation_label, target_id, relations_str in relations:
                cql_r_str = "MATCH (source),(target) " + \
                            "WHERE source.aid = '%s' AND target.aid = '%s' " % (source_aid, target_id) + \
                            "MERGE (source)-[r:%s{%s}]->(target)" % (relation_label, relations_str)
                statement_res = session.run(cql_r_str)
                NeoWrapper.logger.info('Create p2p %s-[%s]-%s relation successfully! Details: %s' %
                                       (source_aid, relation_label, target_id, AcureRateUtils.obj2string(statement_res.consume().counters)))
            pass
        except CypherError as ce:
            NeoWrapper.logger.error('CypherError raised: %s', ce)
        except Exception as e:
            NeoWrapper.logger.error('Exception raised: %s', e)
        finally:
            session.close()

        pass
