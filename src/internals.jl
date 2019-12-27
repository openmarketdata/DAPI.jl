using blpapi, Dates

include("constants.jl")

const default_event_queue = Ref{Ptr{Cvoid}}(0)
default_event_queue_init=false
default_event_queue_count=Dict()
default_event_last=nothing
default_message_handler=Dict()

function __init__()
    global default_event_queue
    global default_message_handler
    ccall((:lfds611_queue_new,blpapi.blpapi3_helper),Cint,(Ptr{Cvoid},Cint),default_event_queue,64)
    for k in [:ADMIN,
              :SESSION_STATUS,
              :SUBSCRIPTION_STATUS,
              :REQUEST_STATUS,
              :RESPONSE,
              :PARTIAL_RESPONSE,
              :SUBSCRIPTION_DATA,
              :SERVICE_STATUS,
              :TIMEOUT,
              :AUTHORIZATION_STATUS,
              :RESOLUTION_STATUS,
              :TOPIC_STATUS,
              :TOKEN_STATUS,
              :REQUEST,
              :UNKNOWN]
        default_message_handler[k]=Dict{Any,Function}()
        default_message_handler[k][""]=x->@debug "$k message received"
    end
end

function default_event_handler(x)
    global default_event_queue
    global default_event_queue_init
    global default_event_queue_count
    global default_event_last
    global default_message_handler
    if !default_event_queue_init # first event received
        ccall((:lfds611_queue_use,blpapi.blpapi3_helper),Cvoid,(Ptr{Cvoid},),default_event_queue)
        default_event_queue_init=true
    end
    events=Ref{Ptr{Cvoid}}(0)
    while 0<ccall((:lfds611_queue_dequeue,blpapi.blpapi3_helper),Cint,(Ptr{Cvoid},Ptr{Cvoid}),default_event_queue[],events)
        event=events[]    
        event_type_code=blpapi_Event_eventType(event)
        event_type=EVENT_TYPE(event_type_code)
        if !(event_type in keys(default_event_queue_count))
            default_event_queue_count[event_type]=0
        end
        default_event_queue_count[event_type]+=1
        default_message_handler[event_type][""](event) # overwrite each event_type's "" function to gain access to raw event
        default_event_last=process_event(event,event_type_code)
        blpapi_Event_release(event)
    end
end

function connect(;host::String="localhost"
                 ,port::Signed=8194
                 ,services::Array{Symbol}=[:refdata]
                 ,timeout::Union{AbstractFloat,Integer}=2.0)
    global default_event_queue
    session_options = blpapi_SessionOptions_create()
    blpapi_SessionOptions_setServerHost(session_options,host)
    blpapi_SessionOptions_setServerPort(session_options,port)
    blpapi_SessionOptions_setAutoRestartOnDisconnection(session_options,true)
    session = blpapi_Session_create(session_options,default_event_handler,C_NULL,default_event_queue[])
    blpapi_SessionOptions_destroy(session_options)
    session_started=blpapi_Session_start(session)
    if session_started!=0
        error("session failed to start")
    end    
    service_dict=Dict{Symbol,NamedTuple}()
    for name in unique(services)
        try
            full_name="//blp/"*string(name)
            service_opened=blpapi_Session_openService(session,full_name)
            if service_opened!=0
                error("service $full_name failed to open")
            end
            @blpapi_setPointer service blpapi_Session_getService(session,service,full_name)
            details=(service=service,)
            operations=get_operations(service)
            details = merge(details,length(operations)>0 ? (operations=operations,) : operations)
            events=get_events(service)
            details = merge(details,length(events)>0 ? (events=events,) : events)
            requests=gen_requests(session,service,operations)
            details = merge(details,length(requests)>0 ? (requests=requests,) : requests)
            subscriptions=gen_subscriptions(session,service,events)
            details = merge(details,length(subscriptions)>0 ? (subscriptions=subscriptions,) : subscriptions)
            service_dict[name]=details
        catch err
            @error err
        end
    end
    return merge((session=session,),NamedTuple{(keys(service_dict)...,)}((values(service_dict)...,)))
end

function disconnect!(connection::Union{Dict{Symbol,Ptr{Nothing}},Dict{Symbol,Any}},services::Array{Symbol}=Symbol[])
    if !(:session in keys(connection)) || connection[:session] == Ptr{Nothing}(0)
        error("not a valid connection")
    end
    session=pop!(connection,:session)
    stop_session=false
    if length(services)==0
        services=keys(connection)
        stop_session=true
    end
    service_dict=Dict(pairs(connection))
    for name in services
        try
            blpapi_Service_release(service_dict[name].service)
            pop!(service_dict,name)
        catch err
            @error err
        end
    end
    if length(service_dict)==0 && stop_session
        blpapi_Session_stop(session)
        session=Ptr{Nothing}(0)
    end
    return merge((session=session,),NamedTuple{(keys(service_dict)...,)}((values(service_dict)...,)))
end

function get_operations(service)
    operations=Dict{Symbol,NamedTuple}()
    for i=0:(blpapi_Service_numOperations(service)-1)
        @blpapi_setPointer operation blpapi_Service_getOperationAt(service,operation,i)
        operation_name=Symbol(blpapi_Operation_name(operation))
        @blpapi_setPointer request blpapi_Operation_requestDefinition(operation,request)
        schema_element=blpapi_SchemaElementDefinition_type(request)
        n=blpapi_SchemaTypeDefinition_numElementDefinitions(schema_element)
        operation_schema=decode_schema_element(schema_element)
        operations[operation_name]=operation_schema
    end
    return NamedTuple{(keys(operations)...,)}((values(operations)...,))
end

function gen_requests(session,service,operations)
    requests=Dict{Symbol,Any}()
    for (request_name,operation) in pairs(operations)      
        request_function=function (;id::Union{Signed,String,Nothing}=nothing,parser::Union{Nothing,Function}=nothing,timeout=2,kwargs...)
            mandatory=Symbol[]
            optional=Symbol[]
            for (k,v) in kwargs
                if !(k in keys(operation))
                    error("'$k' is not a valid parameter")
                end
            end
            for (k,v) in pairs(operation)
                # override for mandatory fields that minValues are not properly populated
                required=false
                if (v.minvalues>0) ||
                   (request_name==:ReferenceDataRequest && k in [:securities,:fields]) ||
                   (request_name==:HistoricalDataRequest && k in [:securities,:fields,:startDate,:endDate]) ||
                   (request_name==:IntradayTickRequest && k in [:security,:startDateTime,:endDateTime])
                    required=true
                    push!(mandatory,k)
                else
                    push!(optional,k)
                end
                if required && length(kwargs)>0 && !(k in keys(kwargs))
                    error("'$k' is mandatory but missing")
                end
                if k in keys(kwargs)
                    if v.maxvalues==1 && isa(kwargs[k],Array)
                        error("'$k' takes one value")
                    elseif v.maxvalues==-1 && !isa(kwargs[k],Array)
                        error("'$k' takes an array")
                    end
                end
            end
            if (length(kwargs)==0)&&(length(mandatory)>0)
                @error "see operations.$request_name for parameter details"
                return (mandatory=mandatory,optional=optional)
            end
            @blpapi_setPointer request blpapi_Service_createRequest(service,request,request_name)
            request_elements=blpapi_Request_elements(request)
            encode_element!(Dict(kwargs),request_elements)
            event_queue=blpapi_EventQueue_create()
            if id==nothing
                id=Dates.value(now())
            end            
            blpapi_Session_sendRequest(session,request,id,C_NULL,event_queue,C_NULL,0)
            blpapi_Request_destroy(request)
            response=retrieve_event(event_queue,:event_queue,Int(timeout*1000),end_event=[:RESPONSE,:UNKNOWN,:TIMEOUT])
            if parser==nothing
                return response
            else
                return parser(response)
            end
        end
        requests[request_name]=request_function
    end
    return NamedTuple{(keys(requests)...,)}((values(requests)...,))
end

function get_events(service)
    events=Dict{Symbol,NamedTuple}()
    for i=0:(blpapi_Service_numEventDefinitions(service)-1)
        @blpapi_setPointer event blpapi_Service_getEventDefinitionAt(service,event,i)
        name=blpapi_SchemaElementDefinition_name(event)
        event_name=Symbol(blpapi_Name_string(name))
        blpapi_Name_destroy(name)
        schema_element=blpapi_SchemaElementDefinition_type(event)
        n=blpapi_SchemaTypeDefinition_numElementDefinitions(schema_element)
        event_schema=decode_schema_element(schema_element)
        events[event_name]=event_schema
    end
    return NamedTuple{(keys(events)...,)}((values(events)...,))
end

function gen_subscriptions(session,service,events)
    global default_message_handler
    subscriptions=Dict{Symbol,Any}()
    for (subscription_name,event) in pairs(events)
        subscription_function= function (;id::Union{Signed,String},kwargs...)
            mandatory=[:topic,:handler]
            optional=[:options,:overrides] 
            if subscription_name=="MarketDataEvents"
                push!(mandatory,:fields)
            else
                push!(optional,:fields)
            end
            if length(kwargs)==0
                @error "$subscription_name has mandatory parameters"
                return (mandatory=mandatory,optional=optional)
            end
            for k in mandatory
                if !(k in keys(kwargs))
                    error("'$k' is mandatory but missing")
                end
            end
            for (k,v) in kwargs
                if !(k in vcat(mandatory,optional))
                    error("'$k' is not a valid parameter")
                end
            end
            list=blpapi_SubscriptionList_create()
            args=Dict(kwargs)
            topic=pop!(args,:topic,"")
            fields=pop!(args,:fields,Array{String,1}())
            options=pop!(args,:options,Array{String,1}())
            handler=pop!(args,:handler,nothing)
            default_message_handler[:SUBSCRIPTION_DATA][id]=handler
            blpapi_SubscriptionList_add(list,topic,id,fields,options,length(fields),length(options))
            blpapi_Session_subscribe(session,list,C_NULL,C_NULL,0)
            blpapi_SubscriptionList_destroy(list)                        
        end
        subscriptions[subscription_name]=subscription_function
    end
    return NamedTuple{(keys(subscriptions)...,)}((values(subscriptions)...,))
end

function decode_datetime(val)
    if Bool(val.offset)
        @debug "found non-zero offset:$(val.offset)"
    end
    if BLPAPI_DATETIME_DATE_PART == val.parts & BLPAPI_DATETIME_DATE_PART
        if (val.year>0) && (val.month>0) && (val.day>0) # value of y,m,d could be 0 even parts are available
            if BLPAPI_DATETIME_TIME_PART == val.parts & BLPAPI_DATETIME_TIME_PART
                return DateTime(val.year,val.month,val.day,val.hours,val.minutes,val.seconds,val.milliSeconds)
            else
                return Date(val.year,val.month,val.day)
            end
        elseif BLPAPI_DATETIME_TIME_PART == val.parts & BLPAPI_DATETIME_TIME_PART
            return Time(val.hours,val.minutes,val.seconds,val.milliSeconds)
        else
            return nothing
        end
    elseif BLPAPI_DATETIME_TIME_PART == val.parts & BLPAPI_DATETIME_TIME_PART
        return Time(val.hours,val.minutes,val.seconds,val.milliSeconds)
    else
        return nothing
    end
end

function decode_value(element,datatype,i)
    if blpapi_Element_numValues(element)==0
        #@warn "empty scalar element with datatype:$datatype"
        if datatype==BLPAPI_DATATYPE_STRING
            return ""
        else
            return nothing
        end
    end    
    if datatype==BLPAPI_DATATYPE_BOOL
        val=Ref{Int32}(0); blpapi_Element_getValueAsBool(element,val,i);
        return Bool(val[])
    elseif datatype==BLPAPI_DATATYPE_CHAR
        error("nyi:BLPAPI_DATATYPE_CHAR")
    elseif datatype==BLPAPI_DATATYPE_BYTE
        error("nyi:BLPAPI_DATATYPE_BYTE")
    elseif datatype==BLPAPI_DATATYPE_INT32
        val=Ref{Int32}(0); blpapi_Element_getValueAsInt32(element,val,i);
        return val[]
    elseif datatype==BLPAPI_DATATYPE_INT64
        val=Ref{Int64}(0); blpapi_Element_getValueAsInt64(element,val,i);
        return val[]
    elseif datatype==BLPAPI_DATATYPE_FLOAT32
        val=Ref{Float32}(0); blpapi_Element_getValueAsFloat32(element,val,i);
        return val[]
    elseif datatype==BLPAPI_DATATYPE_FLOAT64
        val=Ref{Float64}(0); blpapi_Element_getValueAsFloat64(element,val,i);
        return val[]
    elseif datatype==BLPAPI_DATATYPE_STRING
        val=Ref{Ptr{UInt8}}(0); blpapi_Element_getValueAsString(element,val,i);
        return unsafe_string(val[])
    elseif datatype==BLPAPI_DATATYPE_BYTEARRAY
        error("nyi:BLPAPI_DATATYPE_BYTEARRAY")
    elseif datatype==BLPAPI_DATATYPE_DATE
        val=blpapi_Datetime(0,0,0,0,0,0,0,0,0); blpapi_Element_getValueAsDatetime(element,Ref(val),i);
        return decode_datetime(val)
    elseif datatype==BLPAPI_DATATYPE_TIME
        val=blpapi_Datetime(0,0,0,0,0,0,0,0,0); blpapi_Element_getValueAsDatetime(element,Ref(val),i);
        return decode_datetime(val)
    elseif datatype==BLPAPI_DATATYPE_DECIMAL
        error("nyi:BLPAPI_DATATYPE_DECIMAL")
    elseif datatype==BLPAPI_DATATYPE_DATETIME
        val=blpapi_Datetime(0,0,0,0,0,0,0,0,0); blpapi_Element_getValueAsDatetime(element,Ref(val),i);        
        return decode_datetime(val)        
    elseif datatype==BLPAPI_DATATYPE_ENUMERATION
        @debug "BLPAPI_DATATYPE_ENUMERATION as string"
        val=Ref{Ptr{UInt8}}(0); blpapi_Element_getValueAsString(element,val,i);
        return unsafe_string(val[])
    else
        @warn "unknown datatype $datatype as string"
        val=Ref{Ptr{UInt8}}(0); blpapi_Element_getValueAsString(element,val,i);  println(val[]);
        return unsafe_string(val[])
    end
end

function decode_element(element::Ptr{Nothing})
    name=blpapi_Element_nameString(element)
    datatype = blpapi_Element_datatype(element)
    complex=blpapi_Element_isComplexType(element)
    array=blpapi_Element_isArray(element)
    @debug "decoding '$name' as type $datatype, complex $complex, array $array"
    if datatype <= BLPAPI_DATATYPE_ENUMERATION
        if blpapi_Element_isArray(element)
            return (name,[decode_value(element,datatype,i) for i in 0:(blpapi_Element_numValues(element)-1)])
        else
            return (name,decode_value(element,datatype,0))            
        end
    elseif datatype == BLPAPI_DATATYPE_SEQUENCE
        if blpapi_Element_isArray(element)
            return (name,[decode_element(@blpapi_setPointer val blpapi_Element_getValueAsElement(element,val,i)) for i in 0:(blpapi_Element_numValues(element)-1)])
        else
            return (name,[decode_element(@blpapi_setPointer val blpapi_Element_getElementAt(element,val,i)) for i in 0:(blpapi_Element_numElements(element)-1)])
        end
    elseif datatype == BLPAPI_DATATYPE_CHOICE
        return (name,decode_element(@blpapi_setPointer choice blpapi_Element_getChoice(element,choice)))
    elseif datatype == BLPAPI_DATATYPE_CORRELATION_ID
        error("nyi:BLPAPI_CORRELATION_ID")
    end
end

function encode_date(data::Union{Date,DateTime})
    parts=BLPAPI_DATETIME_DATE_PART
    blpapi_Datetime(parts,UInt8(0),UInt8(0),UInt8(0),UInt16(0),UInt8(month(data)),UInt8(day(data)),UInt16(year(data)),Int16(0))
end

function encode_datetime(data::DateTime)
    parts=BLPAPI_DATETIME_DATE_PART | BLPAPI_DATETIME_TIMEMILLI_PART
    blpapi_Datetime(parts,UInt8(hour(data)),UInt8(minute(data)),UInt8(second(data)),UInt16(millisecond(data)),UInt8(month(data)),UInt8(day(data)),UInt16(year(data)),Int16(0))    
end

function encode_value!(data,element,i)
    datatype = blpapi_Element_datatype(element)
    if (data isa Bool) && datatype==BLPAPI_DATATYPE_BOOL 
        blpapi_Element_setValueBool(element,data,i)
    elseif (data isa Char) && datatype==BLPAPI_DATATYPE_CHAR
        blpapi_Element_setValueChar(element,data,i)
    elseif (data isa Signed) && datatype==BLPAPI_DATATYPE_INT32
        blpapi_Element_setValueInt32(element,Int32(data),i)
    elseif (data isa Signed) && datatype==BLPAPI_DATATYPE_INT64
        blpapi_Element_setValueInt64(element,Int64(data),i)
    elseif (data isa AbstractFloat)  && datatype==BLPAPI_DATATYPE_FLOAT32
        blpapi_Element_setValueFloat32(element,Float32(data),i)
    elseif (data isa AbstractFloat) && datatype==BLPAPI_DATATYPE_FLOAT64
        blpapi_Element_setValueFloat64(element,Float64(data),i)
    elseif (data isa String) && datatype==BLPAPI_DATATYPE_STRING 
        blpapi_Element_setValueString(element,data,i)
    elseif (data isa Date) && datatype==BLPAPI_DATATYPE_DATE
        val=encode_date(data)
        blpapi_Element_setValueDatetime(element,Ref(val),i)
    elseif (data isa DateTime) && datatype==BLPAPI_DATATYPE_DATETIME
        val=encode_datetime(data)
        blpapi_Element_setValueDatetime(element,Ref(val),i)
    elseif (data isa Signed) && datatype==BLPAPI_DATATYPE_ENUMERATION
        blpapi_Element_setValueInt32(element,Int32(data),i)
    else
        error("nyi:encode '$(typeof(data))' to datatype $datatype")
    end    
end

function encode_element!(element_dict,element)
    name=blpapi_Element_nameString(element)
    datatype = blpapi_Element_datatype(element)
    complex=blpapi_Element_isComplexType(element)
    array=blpapi_Element_isArray(element)
    @debug "encoding '$name' as type $datatype, complex $complex, array $array"
    if datatype==BLPAPI_DATATYPE_CHOICE
        if isa(element_dict,Dict) || length(element_dict)>1
            error("'$name' must be a dictionary with one item")
        else
            @blpapi_setPointer _element blpapi_Element_setChoice(element,_element,Symbol(name),C_NULL,0)
            @debug "choice '$name'"
            return encode_element!(element_dict[Symbol(name)],_element)
        end
    elseif datatype==BLPAPI_DATATYPE_SEQUENCE
        if blpapi_Element_isComplexType(element) && !blpapi_Element_isArray(element)
            if !isa(element_dict,Dict)
                error("'$name' must be a dictionary")
            end
            for (k,v) in element_dict
                @blpapi_setPointer _element blpapi_Element_getElement(element,_element,k,C_NULL)
                @debug "complex sequence item '$k'"
                encode_element!(v,_element)
            end
        elseif !blpapi_Element_isComplexType(element) && blpapi_Element_isArray(element)
            if !isa(element_dict,Array) && isa(element_dict[1],Dict)
                error("'$name' must be an array of dictionaries")
            end
            n=length(element_dict)
            for i in range(1,length=n)
                @debug "array sequence item '$name', number $i/$n"
                @blpapi_setPointer _element blpapi_Element_appendElement(element,_element)
                encode_element!(element_dict[i],_element)
            end
        else
            error("nyi:sequence is complex and array")
        end
    elseif blpapi_Element_isArray(element)
        if !isa(element_dict,Array)
            error("'$name' must be an array")
        end
        n=length(element_dict)
        for i in range(1,length=n)
            @debug "array item '$name', number $i/$n"
            encode_value!(element_dict[i],element,BLPAPI_ELEMENT_INDEX_END)
        end
    else
        encode_value!(element_dict,element,0)
    end
end

function decode_schema_element(schema_element::Ptr{Nothing})
    schema=Dict{Symbol,NamedTuple}()
    for i=0:(blpapi_SchemaTypeDefinition_numElementDefinitions(schema_element)-1)
        schema_type=blpapi_SchemaTypeDefinition_getElementDefinitionAt(schema_element,i)
        schema_type_name=blpapi_SchemaElementDefinition_name(schema_type)
        schema_element_name=Symbol(blpapi_Name_string(schema_type_name))
        blpapi_Name_destroy(schema_type_name)
        status=blpapi_SchemaElementDefinition_status(schema_type)
        element_type=blpapi_SchemaElementDefinition_type(schema_type)
        minvalues=blpapi_SchemaElementDefinition_minValues(schema_type)
        if minvalues>0 # only a few mandatory fields have minvalues properly set to 1
            @debug "found minvalues: "*string(minvalues)*" at "*string(schema_element_name)
        end
        maxvalues=blpapi_SchemaElementDefinition_maxValues(schema_type)
        if maxvalues==-1
            @debug "found maxvalues: "*string(maxvalues)*" at "*string(schema_element_name)
        end
        description=blpapi_SchemaElementDefinition_description(schema_type)
        element_type_name=blpapi_SchemaTypeDefinition_name(element_type)
        name=blpapi_Name_string(element_type_name)
        datatype=blpapi_SchemaTypeDefinition_datatype(element_type)
        blpapi_Name_destroy(element_type_name)
        if Bool(blpapi_SchemaTypeDefinition_isComplexType(element_type))
            n=blpapi_SchemaTypeDefinition_numElementDefinitions(element_type)    
            complex_type=decode_schema_element(element_type)
        elseif Bool(blpapi_SchemaTypeDefinition_isSimpleType(element_type))
            complex_type=NamedTuple()
        elseif Bool(blpapi_SchemaTypeDefinition_isEnumerationType(element_type))
            complex_type=NamedTuple()
        else
            complex_type=NamedTuple()
        end        
        definition_type=NamedTuple{(:name,:datatype,:enum)}((name,datatype,NamedTuple()))
        schema_element_definition=NamedTuple{(:status,:type,:minvalues,:maxvalues,:description,:complex_type)}((status,definition_type,minvalues,maxvalues,description,complex_type))
        schema[schema_element_name]=schema_element_definition
    end
    return NamedTuple{(keys(schema)...,)}((values(schema)...,))
end

function process_message(message,event_type_code)
    global default_message_handler
    eventType = EVENT_TYPE(event_type_code)
    messageType = blpapi_Message_typeString(message)
    if messageType in ["SlowConsumerWarning","SlowConsumerWarningCleared"]
        @warn "$messageType received"
    end
    n=blpapi_Message_numCorrelationIds(message)
    contents = blpapi_Message_elements(message)
    elements = decode_element(contents)
	if n>0
        t=blpapi_CorrelationId_type(message,0)
        correlationId = blpapi_Message_correlationId(message,0)
        @debug "$messageType with $n correlation id of type $t"
        @debug "$correlationId at index 0"
        if eventType in keys(default_message_handler)
            if correlationId in keys(default_message_handler[eventType])
                default_debug_var=elements
                default_message_handler[eventType][correlationId](elements)
            end
        end
    end
    return elements
end

function process_event(event,event_type_code)
    iterator = blpapi_MessageIterator_create(event)
    done = false
    messages = []
    try
        while !done
            @blpapi_setPointer message blpapi_MessageIterator_next(iterator,message)
            if message == C_NULL
                done = true
            else
                try
                    messages = vcat(messages,process_message(message,event_type_code))
                catch err
                    @error err
                end 
            end
        end
    finally
        blpapi_MessageIterator_destroy(iterator)
    end    
    return messages
end

function retrieve_event(container,container_type,timeout::Int=1000;end_event::Array{Symbol}=[:UNKNOWN,:TIMEOUT,:RESPOND])
    done = false
    events = []
    while !done
        if container_type==:session
            if container==Ptr{Nothing}(0)
                return nothing
            end
            @blpapi_setPointer event blpapi_Session_nextEvent(container,event,timeout)
        elseif container_type==:event_queue
            event=blpapi_EventQueue_nextEvent(container,timeout)
        else
            error("unknown container type: $container_type")
        end
        try
            event_type_code = blpapi_Event_eventType(event)
            event_type=EVENT_TYPE(event_type_code)
            if event_type in [:UNKNOWN,:TIMEOUT]
                @debug "receiving $event_type event"
            end
            events = vcat(events,process_event(event,event_type_code))
            if event_type in end_event
                done = true
            end
        finally
            blpapi_Event_release(event)
        end        
    end
    return events
end
