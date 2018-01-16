import traceback
import os
import uuid

from flask import Flask, request, json
from flasgger import Swagger
from nameko.standalone.rpc import ClusterRpcProxy

from SatoriConfig import GeneralConfig
from entities.acurerate_attributes import P, C
from store.store import Store
from enrichment.enrichment_service import EnrichmentData, EnrichmentBehavior, EnrichmentSource
from enrichment.enrichment_service import EnrichmentService
from importer.csv_contacts_importer import CSVContactsImporter

from utils.acurerate_utils import AcureRateUtils

app = Flask(__name__)
Swagger(app)

@app.route('/api/person/properties', methods=['GET'])
def person_properties_by_email():
    """
    Get a person properties by email
    This endpoint returns all properties of a person by a given EMAIL in a key/value fashion
    ---
    tags:
      - person
    parameters:
      - name: email
        in: query
        type: string
        description: email of person
        required: true
    responses:
      200:
        description: A single user item
        schema:
          properties:
            property-1:
              type: string
              description: A property
              default: 'value-1'
            property-2:
              type: string
              description: A property
              default: 'value-2'
            property-N:
              type: string
              description: A property
              default: 'value-N'
      400:
        description: Bad request. Missing/wrong parameter.
      404:
        description: Person not found
    """
    email = request.args.get('email', None)
    if email is None:
        return 'No email provided', 400

    person = Store.get_person({"email": email})
    if person is None:
        return 'Person with email %s not found' % email, 404

    data = person.get_properties()
    data['aid'] = person.aid
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response

@app.route('/api/person/relations', methods=['GET'])
def person_relations_by_email():
    """
    Get a person relations by email
    This endpoint returns all properties of a person by a given EMAIL in a key/value fashion
    ---
    tags:
      - person
    parameters:
      - name: email
        in: query
        type: string
        description: email of person
        required: true
      - name: filter
        in: query
        type: string
        description: relation type to use as filter (case-insensitive)
    responses:
      200:
        description: A single user item
        schema:
          properties:
            property-1:
              type: string
              description: A property
              default: 'value-1'
            property-2:
              type: string
              description: A property
              default: 'value-2'
            property-N:
              type: string
              description: A property
              default: 'value-N'
      400:
        description: Bad request. Missing/wrong parameter.
      404:
        description: Person not found
    """
    email = request.args.get('email', None)
    if email is None:
        return 'No email provided', 400

    person = Store.get_person({"email": email})
    if person is None:
        return 'Person with email %s not found' % email, 404

    filter = request.args.get('filter', None)

    relations = person.get_relations(filter)
    data = []
    for source_aid, relation_type, target_aid, relation_properties in relations:
        # TODO: move relation_properties from string to array
        data_element = {'relation_type': relation_type, 'relation_properties': relation_properties,
                        'source_id': source_aid, 'target_id': target_aid}
        data.append(data_element)

    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response

@app.route('/api/person/<string:person_id>/properties', methods=['GET'])
def person_properties_by_id(person_id):
    """
    Get a person properties by ID
    This endpoint returns all properties of a person by a given ID in a key/value fashion
    ---
    tags:
      - person
    parameters:
      - name: person_id
        in: path
        type: string
        description: id of person
        required: true
    responses:
      200:
        description: A single user item
        schema:
          properties:
            property-1:
              type: string
              description: A property
              default: 'value-1'
            property-2:
              type: string
              description: A property
              default: 'value-2'
            property-N:
              type: string
              description: A property
              default: 'value-N'
      400:
        description: Bad request
      404:
        description: Person not found
    """
    if len(person_id) == 0:
        return 'Missing person id', 400

    person = Store.get_person_by_aid(person_id)
    if person is None:
        return 'Person with id %s not found' % person_id, 404

    data = person.get_properties()
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response

@app.route('/api/person/<string:person_id>/relations', methods=['GET'])
def person_relations_by_id(person_id):
    """
    Get a person relations by ID
    This endpoint returns all relations of a person by a given ID in a list format
    ---
    tags:
      - person
    parameters:
      - name: person_id
        in: path
        type: string
        description: id of person
        required: true
    responses:
      200:
        description: Returns a list of relations
        schema:
          type: array
          items:
              properties:
                source_id:
                  type: string
                  description: The source id of the relation
                relation_type:
                  type: string
                  description: The type of the relation (e.g. EMPLOYEE_OF, TWITTER_FRIEND, etc.)
                target_id:
                  type: string
                  description: The target id of the relation
                reltion_properties:
                  type: string
                  description: String with comma-separated key:value properties of this relation
      400:
        description: Bad request
      404:
        description: Person not found
    """
    person = Store.get_person_by_aid(person_id)
    if person is None:
        return 'Person with id %s not found' % person_id, 404

    relations = person.get_relations()
    data = []
    for source_aid, relation_type, target_aid, relation_properties in relations:
        # TODO: move relation_properties from string to array
        data_element = {'relation_type': relation_type, 'relation_properties': relation_properties,
                        'source_id': source_aid, 'target_id': target_aid}
        data.append(data_element)
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response
    # return jsonify(data)


@app.route('/api/company/properties', methods=['GET'])
def company_properties_by_domain():
    """
    Get a company properties by DOMAIN
    This endpoint returns all properties of a company by a given DOMAIN in a key/value fashion
    ---
    tags:
      - company
    parameters:
      - name: domain
        in: query
        type: string
        description: domain of a company
        required: true
    responses:
      200:
        description: A single company item
        schema:
          properties:
            property-1:
              type: string
              description: A property
              default: 'value-1'
            property-2:
              type: string
              description: A property
              default: 'value-2'
            property-N:
              type: string
              description: A property
              default: 'value-N'
      400:
        description: Bad request. Missing/wrong parameter.
      404:
        description: Company not found
    """
    domain = request.args.get('domain', None)
    if domain is None:
        return 'No domain provided', 400

    company = Store.get_company({"domain": domain})
    if company is None:
        return 'No company with domain %s found' % domain, 404

    data = company.get_properties()
    data['aid'] = company.aid
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response

@app.route('/api/company/relations', methods=['GET'])
def company_relations_by_domain():
    """
    Get a company relations by DOMAIN
    This endpoint returns all properties of a company by a given DOMAIN in a key/value fashion
    ---
    tags:
      - company
    parameters:
      - name: domain
        in: query
        type: string
        description: domain of a company
        required: true
      - name: filter
        in: query
        type: string
        description: relation type to use as filter (case-insensitive)
    responses:
      200:
        description: A single company item
        schema:
          properties:
            property-1:
              type: string
              description: A property
              default: 'value-1'
            property-2:
              type: string
              description: A property
              default: 'value-2'
            property-N:
              type: string
              description: A property
              default: 'value-N'
      400:
        description: Bad request. Missing/wrong parameter.
      404:
        description: Company not found
    """
    domain = request.args.get('domain', None)
    if domain is None:
        return 'No domain provided. Mandatory parameter', 400

    company = Store.get_company({"domain": domain})
    if company is None:
        return 'No company with domain %s found' % domain, 404

    filter = request.args.get('filter', None)

    data = company.get_relations(filter)
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response

@app.route('/api/company/<string:company_id>/properties', methods=['GET'])
def company_properties_by_id(company_id):
    """
    Get a company properties by ID
    This endpoint returns all properties of a company by a given ID in a key/value fashion
    ---
    tags:
      - company
    parameters:
      - name: company_id
        in: path
        type: string
        description: id of company
        required: true
    responses:
      200:
        description: A single company item
        schema:
          properties:
            property-1:
              type: string
              description: A property
              default: 'value-1'
            property-2:
              type: string
              description: A property
              default: 'value-2'
            property-N:
              type: string
              description: A property
              default: 'value-N'
      404:
        description: Company not found
    """
    company = Store.get_company_by_aid(company_id)
    if company is None:
        return 'Company with id %s not found' % company_id, 404

    data = company.get_properties()
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response

@app.route('/api/company/<string:company_id>/relations', methods=['GET'])
def company_relations_by_id(company_id):
    """
    Get a company relations by ID
    This endpoint returns all relations of a company by a given ID in a list format. Can be filtered.
    ---
    tags:
      - company
    parameters:
      - name: company_id
        in: path
        type: string
        description: id of company
        required: true
    responses:
      200:
        description: Returns a list of relations
        schema:
          type: array
          items:
              properties:
                source_id:
                  type: string
                  description: The source id of the relation
                relation_type:
                  type: string
                  description: The type of the relation (e.g. EMPLOYEE_OF, TWITTER_FRIEND, etc.)
                target_id:
                  type: string
                  description: The target id of the relation
                reltion_properties:
                  type: string
                  description: String with comma-separated key:value properties of this relation
    """
    company = Store.get_company_by_aid(company_id)
    if company is None:
        return 'Company with id %s not found' % company_id, 404

    relations = company.get_relations()
    data = []
    for source_aid, relation_type, target_aid, relation_properties in relations:
        # TODO: move relation_properties from string to array
        data_element = {'relation_type': relation_type, 'relation_properties': relation_properties,
                        'source_id': source_aid, 'target_id': target_aid}
        data.append(data_element)
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response


#@app.route('/api/compute2', methods=['POST'])
def compute():
    """
    Micro Service Based Compute and Mail API
    This API is made with Flask, Flasgger and Nameko
    ---
    parameters:
      - name: body
        in: body
        required: true
        schema:
          id: data
          properties:
            operation:
              type: string
              enum:
                - sum
                - mul
                - sub
                - div
            email:
              type: string
            value:
              type: integer
            other:
              type: integer
    responses:
      200:
        description: Please wait the calculation, you'll receive an email with results
    """
    operation = request.json.get('operation')
    value = request.json.get('value')
    other = request.json.get('other')
    email = request.json.get('email')
    msg = "Please wait the calculation, you'll receive an email with results"
    subject = "API Notification"
    with ClusterRpcProxy(GeneralConfig.AMQP_CONFIG) as rpc:
        # asynchronously spawning and email notification
        rpc.mail.send.async(email, subject, msg)
        # asynchronously spawning the compute task
        result = rpc.compute.compute.async(operation, value, other, email)
        return msg, 200

#@app.route('/api/circles/circle_list', methods=['GET'])
def circles_people():
    """
    Get a list of circles the person is part of (by IDs)
    This endpoint returns all circles of a person
    ---
    tags:
      - circles
    parameters:
      - name: person_id
        in: query
        type: string
        description: source id of path
    responses:
      200:
        description: A list of circles of a person
        schema:
          id: return_test
          properties:
            props:
              type: string
              description: The test
              default: 'test'
            result:
              type: string
              description: The test
              default: 'test'
    """
    # @@@
    person_id = request.json.get('person_id')
    print('Get circle list of person %s' % person_id)

    circles = Store.get_circles(person_id)

@app.route('/api/paths/person_to_person', methods=['GET'])
def person_to_person():
    """
    Get a paths from source person to target person (by IDs)
    This endpoint returns all paths leading from source person to target company via a referral
    ---
    tags:
      - paths
    parameters:
      - name: source_id
        in: query
        type: string
        description: source id of path
      - name: target_id
        in: query
        type: string
        description: target id of path
    responses:
      200:
        description: A list of paths sorted by strength. Each path contains array of segments. Each segment is made of [seg-start, relation-type, seg-end]
        schema:
          type: array
          items:
              properties:
                source_id:
                  type: string
                  description: The source id of the relation
                relation_type:
                  type: string
                  description: The type of the relation (e.g. EMPLOYEE_OF, TWITTER_FRIEND, etc.)
                target_id:
                  type: string
                  description: The target id of the relation
    """
    # Get source/target ids from request
    source_id = request.args.get('source_id', None)
    if source_id is None:
        return 'Missing source id parameter', 400
    target_id = request.args.get('target_id', None)
    if target_id is None:
        return 'Missing target id parameter', 400

    # Check that source/target exist
    if Store.get_person_by_aid(source_id) is None:
        return 'No person matching source id', 400
    if Store.get_person_by_aid(target_id) is None:
        return 'No person matching target id', 400

    try:
        paths = Store.get_paths_to_person(source_id, target_id)
    except Exception as e:
        tb = traceback.format_exc()
        return 'Exception %s raised trying to get path. %s' % (e, tb), 500

    # Return the paths as json with code 200
    response = app.response_class(
        response=json.dumps(paths),
        status=200,
        mimetype='application/json'
    )
    return response


@app.route('/api/paths/person_to_company', methods=['GET'])
def person_to_company():
    """
    Get a paths from source person to target company (by IDs)
    This endpoint returns all paths leading from source person to target company via a referral
    ---
    tags:
      - paths
    parameters:
      - name: source_id
        in: query
        type: string
        description: source id of path
      - name: target_id
        in: query
        type: string
        description: target id of path
      - name: seniority
        in: query
        type: string
        enum:
          - C-Level
          - Senior
          - Not Senior
        description: Level of seniority of people leading to company
      - name: area
        in: query
        type: string
        enum:
          - Board
          - G&A
          - Communications
          - Consulting
          - Customer Service
          - Education
          - Engineering
          - Finance
          - Health Professional
          - Human Resources
          - Information Technology
          - Legal
          - Marketing
          - Operations
          - Product
          - Public Relations
          - Real Estate
          - Recruiting
          - Research
          - Sales
          - Business Development
        description: area of people we want to reach in target company
    responses:
      200:
        description: A list of paths sorted by strength. Each path contains array of segments. Each segment is made of [seg-start, relation-type, seg-end]
        schema:
          type: array
          items:
              properties:
                source_id:
                  type: string
                  description: The source id of the relation
                relation_type:
                  type: string
                  description: The type of the relation (e.g. EMPLOYEE_OF, TWITTER_FRIEND, etc.)
                target_id:
                  type: string
                  description: The target id of the relation
    """
    # Get source/target ids from request
    source_id = request.args.get('source_id', None)
    if source_id is None:
        return 'Missing source id parameter', 400
    target_id = request.args.get('target_id', None)
    if target_id is None:
        return 'Missing target id parameter', 400

    # Check that source/target exist
    if Store.get_person_by_aid(source_id) is None:
        return 'No person matching source id', 400
    if Store.get_company_by_aid(target_id) is None:
        return 'No company matching target id', 400

    # Extract seniority/area filters
    seniority = request.args.get('seniority', None)
    area = request.args.get('area', None)

    try:
        # TODO: instead of 'seniority' & 'area', we may have here a generic k/v property filter
        paths = Store.get_paths_to_company(source_id, target_id, seniority, area)
    except Exception as e:
        tb = traceback.format_exc()
        return 'Exception %s raised trying to get path. %s' % (e, tb), 500

    # Return the paths as json with code 200
    response = app.response_class(
        response=json.dumps(paths),
        status=200,
        mimetype='application/json'
    )
    return response

@app.route('/api/enrichment/providers', methods=['GET'])
def get_providers():
    """
    Get list of providers available in enrichment service
    This endpoint returns list of provider names
    ---
    tags:
      - enrichment
    responses:
      200:
        description: A list of provider names registered to enrichment service
        schema:
          type: array
          items:
            type: string
            default: "provider-name"
    """
    es = EnrichmentService.singleton()
    data = es.get_providers()
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response


@app.route('/api/enrichment/provider_info', methods=['GET'])
def get_provider_info():
    """
    Get provider information
    This endpoint returns list of properties which are the provider information
    ---
    tags:
      - enrichment
    parameters:
      - name: provider_name
        in: query
        type: string
        description: Name of provider to get information on
    responses:
      200:
        description: A list of properties on provider
        schema:
          properties:
            property-1:
              type: string
              description: A property
              default: 'value-1'
            property-2:
              type: string
              description: A property
              default: 'value-2'
            property-N:
              type: string
              description: A property
              default: 'value-N'
      404:
        description: Provider not found
    """
    provider_name = request.args.get('provider_name', None)
    if provider_name is None:
        return 'Missing provider name parameter', 400

    es = EnrichmentService.singleton()
    data = es.get_provider_info(provider_name)
    if data is None:
        return 'Provider %s not found' % provider_name, 404

    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response


@app.route('/api/enrichment/person', methods=['POST'])
def enrich_person_by_key():
    """
    Enrich a person by key
    Provide Key, Data and Behavior for the enrichment process.
    ---
    tags:
      - enrichment
    parameters:
      - name: body
        in: body
        required: true
        schema:
          id: data
          properties:
            key:
              properties:
                email:
                  type: string
                  default: 'email@domain.com'
            data:
              properties:
                first_name:
                  type: string
                last_name:
                  type: string
                email:
                  type: string
                  default: 'email@domain.com'
            behavior:
              properties:
                providers:
                  type: array
                  items:
                    type: string
                    default: "FullContact"
                  description: List of providers
                all_providers:
                  type: boolean
                  default: false
                digest:
                  type: boolean
                  default: true
                enrich_multiple:
                  type: boolean
                  default: false
                create_new:
                  type: boolean
                  default: false
                force_save:
                  type: boolean
                  default: false
                webhook:
                  type: string
                  default: "http://requestb.in/zcr79czc"
    responses:
      200:
        description: Enrichment started. If webhook provided, wait on it for results
      400:
        description: Bad request
      404:
        description: Person not found (Behavior::Create_New = False)
    """
    the_key = request.json.get('key')
    the_data = request.json.get('data', None)
    the_behavior = request.json.get('behavior')

    msg = "Enrichment process started. "
    if 'webhook' in the_behavior:
        msg += 'Wait on webhook %s for results.' % the_behavior['webhook']
    else:
        msg += '(no webhook defined)'

    # Check providers validity
    es = EnrichmentService.singleton()
    providers_list = es.get_providers()
    for p in the_behavior.get('providers', []):
        if p not in providers_list:
            return 'Unknown provider (%s). Aborting enrichment.' % p, 400

    # Prepare the behavior
    eb = EnrichmentBehavior().from_dictionary(the_behavior)

    # Prepare the enrich-data
    if the_data:
        ed = []
        for k, v in the_data.items():
            ed.append(EnrichmentData(k, v, 'override'))
    else:
        ed = None

    # Prepare the enrich-source
    # TODO: complete this...
    the_source = EnrichmentSource('CIA', 'SecretKey')

    # Initialize Enrichment Service
    es = EnrichmentService.singleton()
    es.enrich_person(enrichment_key=the_key, enrichment_data=ed, enrichment_source=the_source, enrichment_behavior=eb)

    subject = "API Notification"
    # with ClusterRpcProxy(GeneralConfig.AMQP_CONFIG) as rpc:
    #     # asynchronously spawning and email notification
    #     rpc.mail.send.async(email, subject, msg)
    #     # asynchronously spawning the compute task
    #     result = rpc.compute.compute.async(operation, value, other, email)
    #     return msg, 200
    return msg, 200


@app.route('/api/enrichment/company', methods=['POST'])
def enrich_company_by_key():
    """
    Enrich a company by Key
    Provide Key, Data and Behavior for the enrichment process.
    ---
    tags:
      - enrichment
    parameters:
      - name: body
        in: body
        required: true
        schema:
          id: data
          properties:
            key:
              properties:
                domain:
                  type: string
                  default: 'domain.com'
            data:
              properties:
                alias:
                  type: string
                  default: 'another-company-name'
                founding_year:
                  type: string
                  default: '2010'
            behavior:
              properties:
                providers:
                  type: array
                  items:
                    type: string
                    default: "FullContact"
                  description: List of providers
                all_providers:
                  type: boolean
                  default: false
                digest:
                  type: boolean
                  default: true
                enrich_multiple:
                  type: boolean
                  default: false
                create_new:
                  type: boolean
                  default: false
                force_save:
                  type: boolean
                  default: false
                webhook:
                  type: string
                  default: http://requestb.in/zcr79czc
    responses:
      200:
        description: Enrichment started. If webhook provided, wait on it for results
      400:
        description: Bad request
      404:
        description: Company not found (Behavior::Create_New = False)
    """
    the_key = request.json.get('key')
    the_data = request.json.get('data', None)
    the_behavior = request.json.get('behavior')

    msg = "Enrichment process started. "
    if 'webhook' in the_behavior:
        msg += 'Wait on webhook %s for results.' % the_behavior['webhook']
    else:
        msg += '(no webhook defined)'

    # Check providers validity
    es = EnrichmentService.singleton()
    providers_list = es.get_providers()
    for p in the_behavior.get('providers', []):
        if p not in providers_list:
            return 'Unknown provider (%s). Aborting enrichment.' % p, 400

    eb = EnrichmentBehavior().from_dictionary(the_behavior)
    #eb.from_dictionary(the_behavior)

    if the_data:
        ed = []
        for k, v in the_data.items():
            ed.append(EnrichmentData(k, v, 'override'))
    else:
        ed = None

    # TODO: complete this...
    the_source = EnrichmentSource('CIA', 'SecretKey')

    # Initialize Enrichment Service
    es = EnrichmentService.singleton()
    es.enrich_company(enrichment_key=the_key, enrichment_data=ed, enrichment_source=the_source, enrichment_behavior=eb)

    subject = "API Notification"
    # with ClusterRpcProxy(GeneralConfig.AMQP_CONFIG) as rpc:
    #     # asynchronously spawning and email notification
    #     rpc.mail.send.async(email, subject, msg)
    #     # asynchronously spawning the compute task
    #     result = rpc.compute.compute.async(operation, value, other, email)
    #     return msg, 200
    return msg, 200

@app.route('/api/importer/import_contacts', methods=['POST'])
def import_contacts():
    """
    Import contacts from file
    Provide path to file, encoding and contacts are imported and enriched
    ---
    tags:
      - importer
    consumes:
      - application/x-www-form-urlencoded
      - multipart/form-data
      - application/json
    produces:
      - application/x-www-form-urlencoded
      - multipart/form-data
    parameters:
      - name: contacts_file
        in: formData
        type: file
        required: true
      - name: user_id
        in: formData
        type: string
        required: true
      - name: encoding
        in: formData
        type: string
        required: true
        default: "utf-8"
      - name: test_mode
        in: formData
        type: boolean
        required: true
        default: true
    responses:
      200:
        description: Please wait the calculation, you'll receive an email with results
    """

    user_id = request.form.get('user_id', None)
    if user_id is None:
        return 'Missing user_id in form parameters', 400
    encoding = request.form.get('encoding', None)
    if encoding is None:
        return 'Missing encoding in form parameters', 400
    test_mode = request.form.get('test_mode', None)
    if test_mode is None:
        return 'Missing test_mode in form parameters', 400
    else:
        test_mode = test_mode in ['True', 'true']

    try:
        file = request.files['contacts_file']
        extension = os.path.splitext(file.filename)[1]
        if extension != '.csv':
            return 'Not a CSV file. Contacts not uploaded', 400
        f_name = str(uuid.uuid4()) + extension
        upload_folder = GeneralConfig.UPLOAD_FOLDER
        file.save(os.path.join(upload_folder, f_name))
        contacts_file_json = json.dumps({'filename': f_name})
    except Exception as e:
        return 'Failed to upload contacts file. Server error: %s' % e, 500

    # Check if user is in DB and has full name
    user_person = Store.get_person_by_aid(user_id)
    if user_person is None:
        return 'Person with id %s not found. Import aborted.' % user_id, 400
    if P.FULL_NAME not in user_person.deduced:
        return 'Person with id %s has no full-name property. Import aborted.' % user_id, 400

    contacts_file_name = '%s\%s' % (GeneralConfig.UPLOAD_FOLDER, f_name)

    print('test_mode = %s, type(test_mode) = %s' % (test_mode, type(test_mode)))

    ci = CSVContactsImporter(path=contacts_file_name,
                             encoding=encoding,
                             source="GoogleContacts",
                             attribution_id=user_person.aid,
                             attribution_name=user_person.deduced[P.FULL_NAME],
                             mapping=CSVContactsImporter.google_mapping2,
                             test_import=test_mode)

    # TODO: have this done async
    ci.import_now()

    return 'Contacts imported successfully', 200


@app.route('/api/importer/import_companies', methods=['POST'])
def import_companies():
    """
    Import companies from file
    Provide path to file, encoding and contacts are imported and enriched
    ---
    tags:
      - importer
    parameters:
      - name: contacts_file
        in: formData
        required: true
        type: file
        consumes: multipart/form-data
      - name: body
        in: body
        required: true
        schema:
          id: data
          properties:
            user_id:
              type: string
              required: true
            encoding:
              type: string
              default: "utf-8"
            test_mode:
              type: boolean
              default: true
    responses:
      200:
        description: Please wait the calculation, you'll receive an email with results
    """
    file_uri = request.json.get('file_uri', None)
    encoding = request.json.get('encoding', 'utf-8')
    user_id = request.json.get('user_id')
    test_mode = request.json.get('test_mode')

    # Check if user is in DB, get his name
    # TODO: implement

    # contacts_file_name = r"C:\temp\AcureRate\Contact Files\%s-google_contacts_export_utf8.csv" % file_prefix
    # ci = CSVContactsImporter(path=contacts_file_name,
    #                          encoding="utf-8",
    #                          source="GoogleContacts",
    #                          attribution_id=person_user.aid,
    #                          attribution_name=full_name,
    #                          mapping=CSVContactsImporter.google_mapping2,
    #                          test_import=False)

    # TODO: have this done async
    # ci.import_now()

    return 'Contacts imported succesfully', 200


app.run(debug=True)