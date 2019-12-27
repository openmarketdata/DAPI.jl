#= Julia 1.0.5 on Windows 10 and WSL Ubuntu 18.04
(v1.0) pkg> add https://github.com/openmarketdata/blpapi.jl
(v1.0) pkg> add https://github.com/openmarketdata/DAPI.jl
=#

using DAPI

@connect(request_session,[:refdata]);
# ReferenceData Request
spx=request_session.refdata.requests.ReferenceDataRequest(id="SPX",securities=["SPX Index"],fields=["INDX_MEMBERS"]);
spx_memb=map(x->x[2][1][2]*" Equity",spx[1][2][2][1][2][5][2][1][2])

# MarketData Subscription
function gen_upd_function(ticker)
    upd=function (x)
        @info "Subscription data received for $ticker"
    end
    return upd
end
subscribe = map(x->(x,gen_upd_function(x)),spx_memb[rand(1:length(spx_memb),5)])

@connect(subscribe_session,[:mktdata]);
for (ticker,upd) in subscribe
    subscribe_session.mktdata.subscriptions.MarketDataEvents(id=ticker,topic=ticker,fields=["BID","ASK"],handler=upd)
end

@disconnect(request_session)
@disconnect(subscribe_session)
