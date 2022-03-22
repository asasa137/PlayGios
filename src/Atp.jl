module Atp

export atp_ranking_get, atp_tourn_get

using Dates
import HTTP
using Gumbo
using Cascadia
using DataFrames

atprankingstart = Date(1973, 8, 23)

function http_get(url)
    rtn = nothing
    try
        rtn = HTTP.get(url)
    catch
        rtn = HTTP.get(url; require_ssl_verification = false)
    end
    return rtn
end

function atp_ranking_get(date_wanted::Date; race=false)
    if date_wanted < atprankingstart
        @warn "$date_wanted < $atprankingstart : " *
          "ranking for atprankingstart will be retrieved"
    end
    url_atp = ""
    sel_player = Selector(".player-cell")
    sel_points = Selector(".points-cell")
    #ranking date
    dropdown_class = "dropdown-holder-wrapper"
    select_dropdown = Selector(".$dropdown_class")
    if race
        url_atp = "https://www.atptour.com/en/rankings/singles-race-to-turin?"  # FIXME to-turin might depends on date
    else
        url_atp = "https://www.atptour.com/en/rankings/singles?"
    end
    http_atp = http_get(url_atp);
    html_str_atp = String(http_atp.body);
    html_atp = parsehtml(html_str_atp);
    dropdown_lists = eachmatch(select_dropdown, html_atp.root)
    dates_dropdown = dropdown_lists[3]
    n_dates = size(Gumbo.children(dates_dropdown[1][2]), 1)
    if size(eachmatch(sel_player, html_atp.root), 1) == 0
        date_wanted -= Day(7)
    end
    ranking_dates = Array{Date}(undef, n_dates)
    for date_i in 1:n_dates
        date_str = replace(text(dates_dropdown[1][2][date_i][1]),
                            r"\r|\t|\s"=>"")
        ranking_dates[date_i] = Date(date_str, dateformat"y.m.d")
    end
    ranking_date_idx = findfirst(date_wanted .- ranking_dates .>= Day(0))
    if isnothing(ranking_date_idx)
        ranking_date = ranking_dates[end]
    else
        ranking_date = ranking_dates[ranking_date_idx]
    end
    url_atp *= "rankDate=" * string(ranking_date) * "&rankRange=1-500"
    http_atp = http_get(url_atp);
    html_str_atp = String(http_atp.body);
    table_start = findfirst("<div id=\"singlesRanking", html_str_atp)[1];
    table_end = findnext("</table>", html_str_atp, table_start)[end];
    table_str = html_str_atp[table_start:table_end];
    table_html = parsehtml(table_str);
    player_table_html = eachmatch(sel_player, table_html.root)
    points_table_html = eachmatch(sel_points, table_html.root)
    rank_df = DataFrame(player=String[], points=Int[])
    for player_i in 1:size(player_table_html, 1)
        if player_i == 1 &&
          occursin(r"[A-Za-z]+", text(points_table_html[player_i]))
            continue
        end
        player = text(player_table_html[player_i][1][1])
        player = replace(player,  r"^[\n\s]*"=>"")
        points_str = text(points_table_html[player_i][1][1])
        points_str = replace(points_str, "," => "")
        points = parse(Int, points_str)
        push!(rank_df, (player, points))
    end
    return rank_df
end

function atp_tourn_get()
    url_atp = "https://www.atptour.com/en/tournaments"
    url_atp = url_atp
    http_atp = http_get(url_atp);
    html_str_atp = String(http_atp.body);
    html_atp = parsehtml(html_str_atp);
    select_tourn = Selector(".tourney-result")
    tourn_list = eachmatch(select_tourn, html_atp.root)
    tourn_GS_img = "/assets/atpwt/images/tournament/badges/categorystamps_grandslam.png"
    tourn_M1000_img = "/assets/atpwt/images/tournament/badges/categorystamps_1000.png"
    tourn_ATPF_img = "/assets/atpwt/images/tournament/badges/categorystamps_finals.svg"
    tourn_df = DataFrame(name=String[], cat=String[], date0=Date[],
                            date1=Date[], url=String[])
    for i_tourn in 1:size(tourn_list, 1)
        select_cat = Selector(".tourney-badge-wrapper")
        select_name = Selector(".tourney-title")
        select_loc = Selector(".tourney-location")
        select_dates = Selector(".tourney-dates")
        tourn_name = eachmatch(select_name, tourn_list[i_tourn])
        tourn_loc = eachmatch(select_loc, tourn_list[i_tourn])
        tourn_loc = text(tourn_loc[1][1])
        tourn_loc = replace(tourn_loc, r"\r|\t|\n"=>"")
        idx0 = findfirst(!isequal(' '), tourn_loc)
        idx1 = findlast(!isequal(' '), tourn_loc)
        tourn_loc = tourn_loc[idx0:idx1]
        tourn_cat = eachmatch(select_cat, tourn_list[i_tourn])
        tourn_dates = eachmatch(select_dates, tourn_list[i_tourn])
        name = text(tourn_name[1][1]) * " (" * tourn_loc * ")"
        name = replace(replace(name, r"\n[\s]*"=>" "), r"^[\s]*"=>"")
        if occursin("Olympic", name)
            tourn_cat_img = "OG"
        else
            tourn_cat_img = Gumbo.attrs(tourn_cat[1][1])["src"]
        end
        if !haskey(Gumbo.attrs(tourn_name[1]), "href")
            continue
        end
        url = Gumbo.attrs(tourn_name[1])["href"]
        url = url
        dates = text(tourn_dates[1][1])
        dates = replace(dates, r"\r|\t|\s"=>"")
        sep_idx = findfirst("-", dates)[1]
        date0 = Date(dates[1:sep_idx-1], dateformat"y.m.d")
        date1 = Date(dates[sep_idx+1:end], dateformat"y.m.d")
        if occursin(tourn_GS_img, tourn_cat_img)
            cat = "GS"
        elseif occursin(tourn_M1000_img, tourn_cat_img)
            cat = "M1000"
        elseif occursin("OG", tourn_cat_img)
            cat = "OG"
        elseif occursin(tourn_ATPF_img, tourn_cat_img)
            cat = "ATPF"
        else
            cat = "other"
        end
        cv_tourn = ["Indian Wells", "Miami", "Monte Carlo", "Madrid"]
        corona_virus = (year(date0) == 2020) && any(occursin.(cv_tourn, name))
        suspended = occursin("Suspended", name)
        postponed = occursin("Postponed", name)
        if cat != "other" && !corona_virus && !suspended && !postponed
            push!(tourn_df, Dict(:name=>name, :cat=>cat, :date0=>date0,
                                    :date1=>date1, :url=>url))
        end
    end
    return tourn_df
end

end
