#method error
curl  localhost:8084/query
#malformed error
curl -XPOST localhost:8084/query
#malformed error
curl -XPOST -d ''  localhost:8084/query
#all objects
curl -XPOST -d '{"query_parameters":{}}' localhost:8084/query
#one object
curl -XPOST -d '{"query_parameters":{"filters": {"oid":"ZTF18ablqckd"}}}' localhost:8084/query
#nobs
curl -XPOST -d '{"query_parameters":{"filters": {"nobs":{"min":5}}}}' localhost:8084/query
curl -XPOST -d '{"query_parameters":{"filters": {"nobs":{"max":5}}}}' localhost:8084/query
curl -XPOST -d '{"query_parameters":{"filters": {"nobs":{"min":5, "max": 10}}}}' localhost:8084/query

#No deberia retornar nada, pero no deberia caerse
curl -XPOST -d '{"query_parameters":{"filters": {"oid":"ZTF18ablqckd","nobs":{"min":5, "max": 10}}}}' localhost:8084/query


#Dates
curl -XPOST -d '{"query_parameters":{"filters": {"dates":{"firstjd":"58000"}}}}' localhost:8084/query
curl -XPOST -d '{"query_parameters":{"filters": {"dates":{"lastjd":"58300"}}}}' localhost:8084/query
curl -XPOST -d '{"query_parameters":{"filters": {"dates":{"firstjd":"58000","lastjd":"58300"}}}}' localhost:8084/query
curl -XPOST -d '{"query_parameters":{"filters": {"nobs":{"min":5, "max": 10},"dates":{"firstjd":"58000","lastjd":"58300"}}}}' localhost:8084/query
curl -XPOST -d '{"query_parameters":{"filters":{"dates":{"deltajd":{"min":"1"}}}}}' localhost:8084/query
curl -XPOST -d '{"query_parameters":{"filters":{"dates":{"deltajd":{"max":"5"}}}}}' localhost:8084/query
curl -XPOST -d '{"query_parameters":{"filters":{"dates":{"deltajd":{"min":"1","max":"5"}}}}}' localhost:8084/query
curl -XPOST -d '{"query_parameters":{"filters":{"nobs":{"min":5, "max": 10},"dates":{"firstjd":"58000","lastjd":"58300","deltajd":{"min":"1","max":"5"}}}}}' localhost:8084/query

#coordinates
curl -XPOST -d '{"query_parameters":{"filters":{"coordinates":{"ra":"30","dec":"50","rs":"100"}}}}' localhost:8084/query

#get detections
curl -XPOST -d '{"oid": "ZTF17aabdwqq"}' localhost:8084/get_detections
#get non_detections
curl -XPOST -d '{"oid": "ZTF17aabdwqq"}' localhost:8084/get_non_detections
