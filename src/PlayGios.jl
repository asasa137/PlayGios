module PlayGios

export matchesupdate, pgrankingupdate, pgstatscompute

include("Atp.jl")

using Statistics
using DataFrames
using CSV
using Dates
import HTTP
using .Atp

savedir = ""

function __init__()
    savedirchange(tempdir() * "/PlayGios")
end

function savedirchange(dir)
    isdir(dir) || mkpath(dir)
    global savedir = dir
end

function matchesupdate()
    col = [:tourney_date => Date[],
                        :winner_name => String[],
                        :loser_name => String[]
                    ]
    typesdict = Dict(first.(col) .=> valtype.(last.(col)))
    fpth = "$savedir/matches.csv"
    if isfile(fpth)
        matches = CSV.read(fpth, DataFrame; types = typesdict)
    else
        matches = DataFrame(col)
        CSV.write(fpth, matches; header = true)
    end
    d0 = isempty(matches) ? Date(1968, 1, 1) : maximum(matches.tourney_date)
    baseurl_atp =
      "https://raw.githubusercontent.com/Tennismylife/TML-Database/master/"
    baseurl_qualchal =
      "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_qual_chall_"
    function tourneydatevecadapt(tourney_date_vec::AbstractVector{<:Union{Missing, Date}})
        for rowid in 1:size(tourney_date_vec, 1)
            if ismissing(tourney_date_vec[rowid])
                val = findlast(.!ismissing.(tourney_date_vec[1:rowid]))
                tourney_date_vec[rowid] = isnothing(val) ?
                  Date(valyear) : tourney_date_vec[val]
            end
        end
        tourney_date_vec = convert(Vector{Date}, tourney_date_vec)
        return tourney_date_vec
    end
    for valyear in year(d0):year(today())
        println("matchesupdate ", valyear)
        url = "$baseurl_atp$valyear.csv"
        pget = HTTP.get(url)
        csv = CSV.read(IOBuffer(String(pget.body)), DataFrame;
          types = typesdict,
          dateformat = Dict(:tourney_date => dateformat"YYYYmmdd"))
        csv.tourney_date = tourneydatevecadapt(csv.tourney_date)
        idx = findall(csv.tourney_date .> d0)
        csv2wr = csv[idx, first.(col)]
        if valyear >= 1978
            url = "$baseurl_qualchal$valyear.csv"
            pget = HTTP.get(url)
            csv = CSV.read(IOBuffer(String(pget.body)), DataFrame;
              types = typesdict,
              dateformat = Dict(:tourney_date => dateformat"YYYYmmdd"))
            csv.tourney_date = tourneydatevecadapt(csv.tourney_date)
            idx = findall(csv.tourney_date .> d0)
            append!(csv2wr, csv[idx, first.(col)])
            sort!(csv2wr, :tourney_date)
        end
        CSV.write(fpth, csv2wr; append = true)
        append!(matches, csv2wr)
    end
    return matches
end

function pgrankingget()
    col = [:date => Date[],
            :pgname => String[],
            :npg => Int16[]
        ]
    typesdict = Dict(first.(col) .=> valtype.(last.(col)))
    fpth = "$savedir/pgranking.csv"
    if isfile(fpth)
        pgranking = CSV.read(fpth, DataFrame; types = typesdict)
    else
        pgranking = DataFrame(col)
        CSV.write(fpth, pgranking; header = true)
    end
    return pgranking
end

function pgrankingupdate()
    # matches.csv update and retrieval
    matches = matchesupdate()
    matches = matches[matches.tourney_date .>= Atp.atprankingstart, :]
    # pgranking retrieval
    fpth = "$savedir/pgranking.csv"
    pgranking = pgrankingget()
    # pgranking computation
    if isempty(pgranking)
        atpranking = atp_ranking_get(Atp.atprankingstart)
        pgname = atpranking.player[1]
        dateidx = findfirst(matches.winner_name .== pgname)
        isnothing(dateidx) && error("invalid pgname=$pgname")
        date = matches.tourney_date[dateidx]
        CSV.write(fpth, Tables.table([date pgname 1]); append = true)
        push!(pgranking, [date pgname 1])
    end
    pgname = pgranking.pgname[end]
    date = pgranking.date[end]
    nextpg_idx = findlast((matches.winner_name .== pgname) .&
      (matches.tourney_date .== date))
    if isnothing(nextpg_idx)
        error("Invalid matches.csv or pgranking.csv")
    end
    while true
        idx = findfirst(matches.loser_name[nextpg_idx:end] .== pgname)
        if isnothing(idx)
            playerretired =
              matches.tourney_date[nextpg_idx] < today() - Year(2) &&
              isempty(findall(matches.winner_name[nextpg_idx+1:end] .== pgname))
            if playerretired
                atpranking = atp_ranking_get(matches.tourney_date[nextpg_idx])
                pgname = atpranking.player[1]
                idx = 1 +
                  findfirst(matches.winner_name[nextpg_idx+1:end] .== pgname)
                isnothing(idx) && error("invalid pgname=$pgname")
                if matches.winner_name[nextpg_idx - 1 + idx] != pgname
                    error("You did something wrong here")
                end
            else
                break
            end
        end
        nextpg_idx = nextpg_idx - 1 + idx
        date = matches.tourney_date[nextpg_idx]
        pgname = matches.winner_name[nextpg_idx]
        npg = sum(pgranking.pgname .== pgname) + 1
        CSV.write(fpth, Tables.table([date pgname npg]); append = true)
        push!(pgranking, [date pgname npg])
        print("pgrankingupdate ", [date pgname npg], "\r")
    end
    println()
    return pgranking
end

function pgstatscompute()
    pgranking = pgrankingget()
    if isempty(pgranking)
        pgranking = pgrankingupdate()
    end
    pgranking.dateout = [pgranking.date[2:end]; today()]
    gd = groupby(pgranking, :pgname)
    pgstats = DataFrame(pgname = [], npg = [], dateinfirst = [],
                        dateoutlast = [], durationmax = [], durationmean = [])
    for gd_i in gd
        pgname = gd_i.pgname[1]
        npg = gd_i.npg[end]
        dateinfirst = gd_i.date[1]
        dateoutlast = gd_i.dateout[end]
        daydiff = getfield.(gd_i.dateout .- gd_i.date, :value)
        durationmax = maximum(daydiff)
        durationmean = mean(daydiff)
        push!(pgstats,
          [pgname npg dateinfirst dateoutlast durationmax durationmean])
    end
    sort!(pgstats, [:npg, :durationmean]; rev = true)
    CSV.write("$savedir/pgstats.csv", pgstats)
    return pgstats
end

end # module
