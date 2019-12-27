#=
(v1.0) pkg> status
    Status `~/.julia/environments/v1.0/Project.toml`
  [1dd7e320] DAPI v0.5.0 #master (https://github.com/openmarketdata/DAPI.jl)
  [a93c6f00] DataFrames v0.20.0
  [a41c293a] blpapi v0.5.0 #master (https://github.com/openmarketdata/blpapi.jl)
=#

using DAPI, DataFrames

function parse_response(response;simple=true)
    if endswith(response[1][1],"Response")
        lvl1=response[1][1]
    else
        error(response[1][1])
    end
    lvls=Dict([("fieldResponse","fieldData")
              ,("ReferenceDataResponse","securityData")
              ,("HistoricalDataResponse","securityData")] )
    lvl2=lvls[lvl1]
    data=[]
    err1=[]
    for item in response
        if item[1]!=lvl1
            @error item[1]
            append!(err1,item)
        elseif item[2][1]!=lvl2
            @error "  "*item[2][1]
            append!(err1,item)
        else
            if lvl1 in ["fieldResponse","ReferenceDataResponse"]
                for x in item[2][2]                    
                    append!(data,[flatten_reqponse(x[2],lvl1)])
                end
            elseif lvl1 in ["HistoricalDataResponse"]
                append!(data,flatten_reqponse(item[2][2],lvl1))
            end
        end
    end
    
    df,err2=fd2df(data)
    if length(err1)>0
        @warn "$(length(err1)) exceptions at $(lvl1) level"
    elseif length(err2)>0
        @warn "$(length(err2)) exceptions at $(lvl2) level"
    end
    if simple        
        return df
    else
        return (df,err1,err2)
    end
end

function flatten_reqponse(item::Array{Tuple{String,Any},1},context::String)
    if context=="fieldResponse"
        # id, fieldInfo
        flat=vcat(item[1],item[2][2])
    elseif context in ["ReferenceDataResponse","HistoricalDataResponse"]
        vals=findall(map(x->x[1] in ["security","sequenceNumber","fieldData"],item))
        # eidData, securityError, fieldExceptions
        errs=findall(map(x->x[1] in ["eidData","securityError","fieldExceptions"],item))
        for i in errs
            if length(item[i][2])>0
                @warn "$(item[vals[1]])-$(item[i][1]): $(item[i][2])"
            end
        end
        if context=="ReferenceDataResponse"
            field=item[vals[3]][2]
            field_with_bulk= Array{Tuple{String,Any},1}()
            for f in field
                if f[1] in BULK_FIELDS
                    if !(f[1] in BULK_FIELDS_TESTED)
                        @warn "untested bulk field $(f[1]) detected"
                    end
                    bf,err2=fd2df(map(x->x[2],f[2]))
                    push!(field_with_bulk,(f[1],bf))
                else
                    push!(field_with_bulk,f)
                end
            end
            flat=vcat(item[vals[1]],item[vals[2]],field_with_bulk)
        elseif context=="HistoricalDataResponse"
            flat=map(x->vcat(item[vals[1]],item[vals[2]],x),map(x->x[2],item[vals[3]][2]))
        end
    else
        error("$(item[1]) not supported")
    end
    return flat
end

function fd2df(data)
    df=DataFrame()
    err2=[]
    for item in data        
        if size(df,2)==0
            df=allowmissing!(DataFrame(map(x->[x[2]],item),map(x->Symbol(x[1]),item)))
        else
            df_add=allowmissing!(DataFrame(map(x->[x[2]],item),map(x->Symbol(x[1]),item)))
            # TODO: add a function to DataFrames to merge/union two dataframes
            cols=names(df)
            cols_add=names(df_add)
            miss_cols=setdiff(cols,cols_add)
            new_cols=setdiff(cols_add,cols)
            if length(miss_cols)>0
                df_add[:,miss_cols]=missing
            end
            if length(new_cols)>0                
                df=hcat(df,repeat(first(df_add[:,new_cols],1),size(df,1)))
                df[:,new_cols]=missing
            end
            try
                append!(df,df_add[:,vcat(cols,new_cols)])
            catch e
                @error e
                @error df_add
                append!(err2,item)
            end
        end
    end
    return (df,err2)
end

@connect(request_session,[:refdata]);

history=request_session.refdata.requests.HistoricalDataRequest(securities=["GE US Equity","GM US Equity"],
                                                               startDate="20191201",
                                                               endDate="20191231",
                                                               fields=["PX_OPEN","PX_LAST","EQY_WEIGHTED_AVG_PX"],
                                                               parser=parse_response)

@disconnect(request_session)


