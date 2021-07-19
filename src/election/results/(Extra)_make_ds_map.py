import folium
import branca.colormap as cm
from folium.plugins import FastMarkerCluster


def generate_base_map(default_location=[-22.010147, -47.890706], default_zoom_start=5):
    base_map = folium.Map(location=default_location, zoom_start=default_zoom_start)
    return base_map


def generate_plot(clusters_map, data, output_filepath, integrity_score):
    callback = (
        "function (row) {"
        "var circle = L.circle(new L.LatLng(row[0], row[1]));"
        "return circle};"
    )

    clusters_map.add_child(
        FastMarkerCluster(data[["lat", "lon"]].values.tolist(), callback=callback)
    )
    clusters_map.save(
        outfile=output_filepath
        + "IS_"
        + str(round(integrity_score, 5))
        + "/polling_places_map.html"
    )


def generate_plot_2(
    data, filtered_data, path_geojson, digital_mesh, output_filepath, integrity_score
):
    map = generate_base_map()
    digital_mesh.rename(columns={"CD_GEOCMU": "COD_LOCALIDADE_IBGE"}, inplace=True)
    digital_mesh["COD_LOCALIDADE_IBGE"] = digital_mesh["COD_LOCALIDADE_IBGE"].astype(
        "float"
    )

    list_city = []
    list_pl = []
    for index, city_data in data.groupby("COD_LOCALIDADE_IBGE"):
        list_city.append(index)
        list_pl.append(len(city_data))

    all_city_data = {"COD_LOCALIDADE_IBGE": list_city, "all_polling_places": list_pl}
    all_city_data = pd.DataFrame(all_city_data)

    list_city = []
    list_pl = []
    for index, city_data in filtered_data.groupby("COD_LOCALIDADE_IBGE"):
        list_city.append(index)
        list_pl.append(len(city_data))

    filtered_city_data = {
        "COD_LOCALIDADE_IBGE": list_city,
        "filtered_polling_places": list_pl,
    }

    filtered_city_data = pd.DataFrame(filtered_city_data)

    city_data = all_city_data.merge(
        filtered_city_data, on="COD_LOCALIDADE_IBGE", how="left"
    )

    city_data = digital_mesh.merge(city_data, on="COD_LOCALIDADE_IBGE", how="left")

    city_data["filtered_polling_places"].fillna(0, inplace=True)
    city_data["pl_perc"] = (
        100 * city_data["filtered_polling_places"] / city_data["all_polling_places"]
    )
    city_data["COD_LOCALIDADE_IBGE"] = city_data["COD_LOCALIDADE_IBGE"].astype("int64")

    city_data.rename(columns={"COD_LOCALIDADE_IBGE": "CD_GEOCMU"}, inplace=True)
    city_data["pl_perc"].fillna(0, inplace=True)

    step = cm.LinearColormap(
        ["#F9F9F3", "#bedbea", "#59a2cb", "#2166ac"],
        vmin=city_data["pl_perc"].min(),
        vmax=city_data["pl_perc"].max(),
    )

    folium.GeoJson(
        data=city_data,
        name="Br Cities",
        tooltip=folium.GeoJsonTooltip(
            fields=["NM_MUNICIP", "pl_perc"],
            aliases=[
                '<div style="background-color: lightyellow; color: black; padding: 3px; border: 2px solid black; border-radius: 3px;">'
                + item
                + "</div>"
                for item in ["City", "Percentual"]
            ],
            labels=True,
            sticky=True,
        ),
        style_function=lambda city: {
            "fillColor": step(city["properties"]["pl_perc"]),
            "color": "black",
            "fillOpacity": 0.7,
            "lineOpacity": 0.1,
            "weight": 0.3,
        },
    ).add_to(map)

    map.add_child(step)

    # FloatImage('https://i.ibb.co/pRQmCBR/legend-hotspots.png', bottom=75, left=80).add_to(m)

    # folium.LayerControl().add_to(m)
    # Save to html
    return m
