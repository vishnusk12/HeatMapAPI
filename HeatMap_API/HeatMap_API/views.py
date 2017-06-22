'''
Created on Dec 14, 2016

@author: vishnu.sk
'''
from rest_framework import viewsets
from rest_framework.response import Response
from rest_framework.decorators import permission_classes
from rest_framework import permissions
from pymongo import MongoClient
import math

ListingType = ['Closed_Listings', 'Active_Listings']
SpanType = ['3M', '6M', '1Y', '3Y', '5Y']
AtrType = ['ClosePrice', 'PriceSqft', 'NumberOfTransactions', 'ListPrice', 'iEstimate', 'Price_trend']


@permission_classes((permissions.AllowAny,))
class HeatMapCountyStateAPI(viewsets.ViewSet):
    def list(self, request):

        """
        HeatMap_County_State_API: (HeatMap):
        API will return state_wise and county_wise attribute value, coordinates, margin of corresponding listings,span and attribute.

        url:https://insights.propmix.io:8006//county_state_data/?listings=Closed_Listings&span=5Y&attribute=ClosePrice

        ---
        # YAML (must be separated by `---`)

        type:
          schema : { CountyData:{ Margin:[], type: string , features:[{geometry:{type: string, coordinates:[[[]]]}, type: string, id: string, properties:{ county: string, attribute: string}}
          features:
            required: false
            type: string
        parameters_strategy: merge
        omit_parameters:
            - path
        parameters:
            - name: listings
              description: To determine whether its a sold property or active property.
              required: true
              type: string
              paramType: query
              defaultValue: Closed_Listings
            - name: span
              description: It is the time span in which median attribute values are calculated such as 3M,6M,1Y,3Y and 5Y.
              required: true
              paramType: query
              defaultValue: 3Y
            - name: attribute
              description: Median Value of ClosePrice, PriceSqft, NumberOfTransactions, ListPrice, iEstimate and Price_trend of properties in zip,county or state of corresponding span.
              required: true
              paramType: query

            responseMessages:
            - code: 401
              message: Not authenticated
            - code : 404
              message: Not Found
            - code : ERROR
              message : No Zip Specified or Invalid Zip

        consumes:
            - application/json
            - application/xml
        produces:
            - application/json
            - application/xml
        """

        try:
            if request.query_params['listings'] not in ListingType:
                return Response({'status': 'ERROR', 'error': 'INVALID_LISTING'})
            elif request.query_params['attribute'] not in AtrType:
                return Response({'status': 'ERROR', 'error': 'INVALID_ATTRIBUTE_NAME'})
            elif request.query_params['span'] not in SpanType:
                return Response({'status': 'ERROR', 'error': 'INVALID_SPAN'})
        except:
                return Response({'status': 'ERROR', 'error': 'NO_LISTING_SPECIFIED'})
        req_listing = request.query_params['listings']
        req_span = request.query_params['span']
        req_attr = request.query_params['attribute']
        db_client = MongoClient(host='52.91.122.15', port=27017)
        level_map = {"County": {"collection": "HeatMapMediansCountyWise_mlslite"}, "State": {"collection": "HeatMapMediansStateWise_mlslite"}}
        level_map_cords = {"County": {"collection": "CountyWise_coordinates"}, "State": {"collection": "StateWise_coordinates"}}

        class DBFilter():
            @staticmethod
            def filter(level, listings, span, attribute):

                features_list = []
                data_ = list(db_client["Heat_Map"][level_map[level]["collection"]].find({}, {"%s.%s.%s" % (listings, span, attribute): True}))
                data_cords = list(db_client["iestimate"][level_map_cords[level]["collection"]].find({}, {"coordinates": True}))
                if listings == 'Closed_Listings':
                    level_map_margin = {"County": {"collection": "Closed_Listings_Margin_county"}, "State": {"collection": "Closed_Listings_Margin_state"}}
                elif listings == 'Active_Listings':
                    level_map_margin = {"County": {"collection": "Active_Listings_Margin_county"}, "State": {"collection": "Active_Listings_Margin_state"}}
                data_margin = list(db_client["Heat_Map"][level_map_margin[level]["collection"]].find({"Span": span, "Attribute": attribute}, {"margin": 1}))
                Margin_list = []
                for value in data_margin:
                    Margin_list.append(value["margin"])
                for i in range(len(data_)):
                    try:
                        CountyOrState = data_[i]['_id'][level][0] if type(data_[i]['_id'][level]) is list else data_[i]['_id'][level]
                        if math.isnan(float(data_[i][listings][span][attribute])):
                            span_attr_value = -1
                        else:
                            span_attr_value = data_[i][listings][span][attribute]
                        prop = {level: CountyOrState, attribute: span_attr_value}
                        cords = data_cords[i]['coordinates']
                        polygon = {'type': 'Polygon', 'coordinates': cords[0]}
                        features_list.append({'type': span, 'id': i, 'properties': prop, 'geometry': polygon})
                    except:
                        print 'no data for', level, CountyOrState
                result = {'type': "FeatureCollection", 'features': features_list, 'Margin': Margin_list[0]}
                return result

        State_data = DBFilter.filter("State", req_listing, req_span, req_attr)
        County_data = DBFilter.filter("County", req_listing, req_span, req_attr)
        all_data = {"CountyData": County_data, "StateData": State_data}
        return Response(all_data)


@permission_classes((permissions.AllowAny,))
class HeatMapZipAPI(viewsets.ViewSet):

    def list(self, request):
        """
                HeatMap_Zip_API: (HeatMap):
                API will return zip_wise attribute value, coordinates, margin of corresponding listings ,span and attribute within the given radius of inputted latitude and longitude.

                url:https://insights.propmix.io:8006/zip-wise_data/?listings=Active_Listings&radius=5&latitude=42.355812&longitude=-71.073302&span=5Y&attribute=ListPrice

                ---
                # YAML (must be separated by `---`)

                type:
                  schema : { Margin:[], type: string , features:[{geometry:{type: string, coordinates:[[]]}, type: string, id: string, properties:{ county: string, State: string, PostalCode: string, attribute:[] }}
                  features:
                    required: false
                    type: string
                parameters_strategy: merge
                omit_parameters:
                    - path
                parameters:
                    - name: listings
                      description: To determine whether its a sold property or active property.
                      required: true
                      type: string
                      paramType: query
                      defaultValue: Active_Listings
                    - name: span
                      description: It is the time span in which median attribute values are calculated such as 3M, 6M, 1Y, 3Y and 5Y.
                      required: true
                      paramType: query
                      defaultValue: 5Y
                    - name: attribute
                      description: Median Value of ClosePrice, PriceSqft, NumberOfTransactions, ListPrice, iEstimate and Price_trend of properties in zip,county or state of corresponding span.
                      required: true
                      paramType: query
                      defaultValue: ListPrice
                    - name: radius
                      description: Circular distance from a particular property of which datas to be returned.
                      required: true
                      paramType: query
                      defaultValue: 5
                    - name: latitude
                      description: Latitude of a property
                      required: true
                      paramType: query
                      defaultValue: 42.355812
                    - name: longitude
                      description: Longitude of a property
                      required: true
                      paramType: query
                      defaultValue: -71.073302

                    responseMessages:
                    - code: 401
                      message: Not authenticated
                    - code : 404
                      message: Not Found
                    - code : ERROR
                      message : No Zip Specified or Invalid Zip

                consumes:
                    - application/json
                    - application/xml
                produces:
                    - application/json
                    - application/xml
                """
        try:
            if request.query_params['listings'] not in ListingType:
                return Response({'status': 'ERROR', 'error': 'INVALID_LISTING'})
            elif request.query_params['span'] not in SpanType:
                return Response({'status': 'ERROR', 'error': 'INVALID_SPAN'})
            elif request.query_params['attribute'] not in AtrType:
                return Response({'status': 'ERROR', 'error': 'INVALID_ATTRIBUTE_NAME'})
        except:
                return Response({'status': 'ERROR', 'error': 'NO_SPAN_SPECIFIED'})
        req_listings = request.query_params['listings']
        req_span = request.query_params['span']
        req_attr = request.query_params['attribute']
        radius = request.query_params['radius']
        try:
            if not request.query_params['radius'].isdigit():
                return Response({'status': 'ERROR', 'error': 'INVALID_radius'})
        except:
                return Response({'status': 'ERROR', 'error': 'NO_radius_SPECIFIED'})
        longitude = " "
        if ("longitude" in request.query_params):
            longitude = request.query_params['longitude']
        else:
            return Response({'status': 'ERROR', 'error': 'Please Enter a valid longitude'})
        latitude = " "
        if ("latitude" in request.query_params):
            latitude = request.query_params['latitude']
        else:
            return Response({'status': 'ERROR', 'error': 'Please Enter a valid latitude'})
        db_client = MongoClient(host='52.91.122.15', port=27017)
        lat = float(latitude)
        longi = float(longitude)
        rad = int(radius)
        span = str(req_span)
        attribute = str(req_attr)
        listings = str(req_listings)
        data_search = list(db_client.Heat_Map.HeatMap_Radius_Search.find({"loc": {"$geoWithin": {"$centerSphere": [[longi, lat], rad / 3963.2]}}}))
        if listings == 'Closed_Listings':
            data_margin = list(db_client.Heat_Map.Closed_Listings_Margin_zip.find({"Span": span, "Attribute": attribute}, {"margin": 1}))
        elif listings == 'Active_Listings':
            data_margin = list(db_client.Heat_Map.Active_Listings_Margin_zip.find({"Span": span, "Attribute": attribute}, {"margin": 1}))
        Margin_list = []
        for value in data_margin:
            Margin_list.append(value["margin"])
        data_zip = []
        for list_value in data_search:
            data_zip.append(list_value["PostalCode"])
        uniq_zip = list(set(data_zip))
        zip_data_list = []
        features_list = []
        for indx, i in enumerate(uniq_zip):
            zip_data_dict = {}
            data_post = list(db_client.Heat_Map.HeatMap_Radius_Search.find({"PostalCode": i}))
            cords = []
            span_attr_value = []
            for value1 in data_post:
                cords.append(value1['loc']['coordinates'])
            span_attr_value.append(value1[listings][span][attribute])
            print span_attr_value
            if span_attr_value[0] == None:
                span_attr_value = -1
            elif math.isnan(float(span_attr_value[0])):
                span_attr_value = -1
            else:
                span_attr_value = span_attr_value[0]
            zip_data_dict = data_post[0]
            zip_data_dict.pop('loc', None)
            zip_data_dict.pop('_id', None)
            zip_data_dict.pop('1M', None)
            zip_data_dict.pop('3M', None)
            zip_data_dict.pop('6M', None)
            zip_data_dict.pop('1Y', None)
            zip_data_dict.pop('5Y', None)
            zip_data_dict.pop('Closed_Listings', None)
            zip_data_dict.pop('Active_Listings', None)
            zip_data_dict[attribute] = span_attr_value
            zip_data_list.append(zip_data_dict)
            polygon = {'type': 'Polygon', 'coordinates': cords}
            features_list.append({'type': span, 'id': indx, 'properties': zip_data_list[indx], 'geometry': polygon})
        result = {'type': "FeatureCollection", 'features': features_list, 'Margin': Margin_list[0]}
        print result
        return Response(result)
