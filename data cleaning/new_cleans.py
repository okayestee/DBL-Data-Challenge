import json

def read_file(path):
    with open(path, 'r') as file:
        new_file = json.load(file)
    return new_file

def remove_variables(tweet: dict) -> dict:
    variables_to_remove: list[str] = ["source", "location", "url", "protected", "utc_offset", "time_zone", "geo_enabled", "contributors_enabled", "is_translator", "profile_background_color", "profile_background_image_url", "profile_background_image_url_https", "profile_background_tile", "profile_link_color", "profile_sidebar_border_color", "profile_sidebar_fill_color", "profile_text_color", "profile_use_background_image", "profile_image_url", "profile_image_url_https", "profile_banner_url", "default_profile", "default_profile_image", "following", "follow_request_sent", "notifications"]

    keys_to_remove = list()
    for key in tweet:
        if type(tweet[key]) == dict:
            remove_variables(tweet[key])
        if key in variables_to_remove:                
            keys_to_remove.append(key)

    for key in keys_to_remove:
        del tweet[key]
    return tweet

            
    

#print(read_file('data cleaning/Test tweet 4'))
print(remove_variables(read_file('data cleaning/Test tweet 4')[0]))