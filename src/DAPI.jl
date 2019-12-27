module DAPI

include("internals.jl")

macro connect(connection,services,timeout)
    quote
        $(esc(connection))=connect(services=$(esc(services)),timeout=$(esc(timeout)))
        return nothing
    end
end

macro connect(connection,services)
    quote
        $(esc(connection))=connect(services=$(esc(services)))
        return nothing
    end
end

macro connect(connection)
    quote
        $(esc(connection))=connect()
        return nothing
    end
end

macro disconnect(connection,services)
    quote
        connection_dict=Dict(pairs($(esc(connection))))
        service_list=$(esc(services))
        $(esc(connection))=disconnect!(connection_dict,service_list)
        return nothing
    end
end

macro disconnect(connection)
    quote
        $(esc(connection))=disconnect!(Dict(pairs($(esc(connection)))))
        return nothing
    end
end

export @connect,@disconnect

end # module
