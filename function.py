import json
import requests
import logging
import time
import os
import boto3

# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

# Get environment variables
PASSWORD = os.environ.get('SHOPIFY_PASSWORD')
SHOP_NAME = os.environ.get('SHOPIFY_SHOP_NAME', 'roof-top-shop')

# GraphQL API endpoint
url = f'https://{SHOP_NAME}.myshopify.com/admin/api/2024-10/graphql.json'
headers = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": PASSWORD
}

def send_request_with_retry(query, max_retries=3, delay=2):
    """Send a request with retry logic."""
    for attempt in range(max_retries):
        try:
            logger.info(f"Sending GraphQL request (attempt {attempt + 1}/{max_retries})")
            response = requests.post(url, json=query, headers=headers, timeout=10)
            
            if response.status_code == 200:
                response_json = response.json()
                # Check for GraphQL errors inside a 200 response
                if 'errors' in response_json:
                    logger.warning(f"GraphQL errors in response: {json.dumps(response_json['errors'])}")
                
                # Add a small delay even after successful requests to avoid overloading the API
                time.sleep(0.5)
                return response_json
            else:
                logger.warning(f"Attempt {attempt + 1} failed with status {response.status_code}: {response.text}")
                if attempt < max_retries - 1:  # Don't sleep on the last attempt
                    time.sleep(delay * (attempt + 1))  # Exponential backoff
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed with exception: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(delay * (attempt + 1))
    
    logger.error("All retries failed.")
    return None


def get_fulfillment_order_details(fulfillment_order_id):
    """
    Get the fulfillment order details including line items, available locations,
    whether items are length-transport items, and daktrim_koppelstukje flags.
    """
    query = {
        "query": """
        query {
          fulfillmentOrder(id: "%s") {
            id
            status
            lineItems(first: 50) {
              edges {
                node {
                  id
                  lineItem {
                    quantity
                    id
                    name
                    sku
                    variant {
                      metafields(namespace: "fulfillment_system", first: 10) {
                        nodes {
                          key
                          value
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """ % fulfillment_order_id
    }
    
    logger.info(f"Fetching fulfillment order details for {fulfillment_order_id}")
    result = send_request_with_retry(query)
    
    if not result or 'errors' in result or not result.get('data', {}).get('fulfillmentOrder'):
        if 'errors' in result:
            logger.error(f"GraphQL errors: {json.dumps(result['errors'])}")
        else:
            logger.error("Failed to get fulfillment order details")
        return None
    
    # Parse the response to extract line items with their details
    parsed_data = {
        'fulfillment_order_id': fulfillment_order_id,
        'status': result['data']['fulfillmentOrder']['status'],
        'line_items': []
    }
    
    edges = result['data']['fulfillmentOrder']['lineItems']['edges']
    
    if not edges:
        logger.info(f"Fulfillment order {fulfillment_order_id} has no line items")
        return parsed_data
        
    logger.info(f"Found {len(edges)} line items in the fulfillment order")
    
    for edge in edges:
        node = edge['node']
        line_item = node['lineItem']
        
        # Initialize metafield values
        available_locations = []
        is_length_transport = False
        is_daktrim_koppelstukje = False
        
        if line_item.get('variant', {}).get('metafields', {}).get('nodes'):
            metafields = line_item['variant']['metafields']['nodes']
            
            for metafield in metafields:
                key = metafield.get('key', '')
                value = metafield.get('value', '')
                
                if key == 'availability_location':
                    try:
                        # Ensure available_locations is a list
                        available_locations = json.loads(value)
                        if not isinstance(available_locations, list):
                            available_locations = [available_locations] if available_locations else []
                    except Exception as e:
                        logger.warning(f"Failed to parse locations from metafield: {str(e)}")
                        available_locations = []
                
                elif key == 'length_transport':
                    try:
                        is_length_transport = (value.lower() == 'true')
                    except Exception as e:
                        logger.warning(f"Failed to parse length transport flag: {str(e)}")
                        is_length_transport = False
                
                elif key == 'daktrim_koppelstukje':
                    try:
                        is_daktrim_koppelstukje = (value.lower() == 'true')
                    except Exception as e:
                        logger.warning(f"Failed to parse daktrim_koppelstukje flag: {str(e)}")
                        is_daktrim_koppelstukje = False
        
        parsed_data['line_items'].append({
            'fulfillment_order_line_item_id': node['id'],
            'line_item_id': line_item['id'],
            'name': line_item['name'],
            'sku': line_item.get('sku', ''),
            'quantity': line_item['quantity'],
            'available_locations': available_locations,
            'is_length_transport': is_length_transport,
            'is_daktrim_koppelstukje': is_daktrim_koppelstukje
        })
    
    return parsed_data

def categorize_items(order_details):
    """
    Categorize items based on the decision tree into:
    - Length products at Rooftopshop Magazijn
    - Non-length products at Rooftopshop Magazijn
    - Length products at external locations
    - Non-length products at external locations
    - Daktrim koppelstukje products at Rooftopshop Magazijn
    """
    # Initialize categories
    categories = {
        "length_rooftopshop": [],      # Length products at Rooftopshop
        "non_length_rooftopshop": [],  # Non-length products at Rooftopshop
        "length_external": [],         # Length products at external locations
        "non_length_external": [],     # Non-length products at external locations
        "daktrim_koppelstukje_rooftopshop": []  # Daktrim koppelstukje products at Rooftopshop
    }
    
    # External locations tracking
    external_locations = set()
    
    # Group items into categories
    for item in order_details.get('line_items', []):
        # Retrieve available locations - ensure it's a list
        available_locations = item.get('available_locations', [])
        if not isinstance(available_locations, list):
            available_locations = [available_locations] if available_locations else []
        
        # Check if Rooftopshop is in available locations
        is_at_rooftopshop = "Rooftopshop Magazijn" in available_locations
        
        # Track external locations if applicable
        if not is_at_rooftopshop and available_locations:
            external_locations.update(loc for loc in available_locations if loc != "Rooftopshop Magazijn")
        
        # Safely extract flags - ensure they're booleans
        is_length_transport = item.get('is_length_transport', False)
        if not isinstance(is_length_transport, bool):
            # Try to convert to boolean if it's a string
            if isinstance(is_length_transport, str):
                is_length_transport = is_length_transport.lower() == 'true'
            else:
                is_length_transport = bool(is_length_transport)
                
        is_daktrim_koppelstukje = item.get('is_daktrim_koppelstukje', False)
        if not isinstance(is_daktrim_koppelstukje, bool):
            # Try to convert to boolean if it's a string
            if isinstance(is_daktrim_koppelstukje, str):
                is_daktrim_koppelstukje = is_daktrim_koppelstukje.lower() == 'true'
            else:
                is_daktrim_koppelstukje = bool(is_daktrim_koppelstukje)
        
        # Prepare item details for categorization
        item_details = {
            "id": item.get('fulfillment_order_line_item_id'),
            "quantity": item.get('quantity', 0),
            "locations": available_locations,
            "is_length_transport": is_length_transport,
            "is_daktrim_koppelstukje": is_daktrim_koppelstukje
        }
        
        # Special case: Daktrim koppelstukje at Rooftopshop Magazijn
        if is_daktrim_koppelstukje and is_at_rooftopshop:
            categories["daktrim_koppelstukje_rooftopshop"].append(item_details)
            continue
            
        # Categorize based on type and location
        if is_length_transport:
            if is_at_rooftopshop:
                categories["length_rooftopshop"].append(item_details)
            else:
                categories["length_external"].append(item_details)
        else:  # Non-length items
            if is_at_rooftopshop:
                categories["non_length_rooftopshop"].append(item_details)
            else:
                categories["non_length_external"].append(item_details)
    
    # Add summary info
    categories["summary"] = {
        "has_length_items": len(categories["length_rooftopshop"]) + len(categories["length_external"]) > 0,
        "has_daktrim_koppelstukje_items": len(categories["daktrim_koppelstukje_rooftopshop"]) > 0,
        "all_items_at_rooftopshop": len(categories["length_external"]) + len(categories["non_length_external"]) == 0,
        "all_non_length_at_rooftopshop": len(categories["non_length_external"]) == 0,
        "external_locations": list(external_locations)
    }
    
    # Log categorization
    if 'logger' in globals():
        logger.info("Item categorization summary:")
        logger.info(f"Length items at Rooftopshop: {len(categories['length_rooftopshop'])}")
        logger.info(f"Non-length items at Rooftopshop: {len(categories['non_length_rooftopshop'])}")
        logger.info(f"Length items at external locations: {len(categories['length_external'])}")
        logger.info(f"Non-length items at external locations: {len(categories['non_length_external'])}")
        logger.info(f"Daktrim koppelstukje items at Rooftopshop: {len(categories['daktrim_koppelstukje_rooftopshop'])}")
        logger.info(f"External locations: {categories['summary']['external_locations']}")
    
    return categories

def group_by_external_location(items):
    """
    Group items by their external locations for splitting.
    Returns a dictionary with location name as key and items as values.
    """
    location_groups = {}
    
    for item in items:
        # For items with multiple locations, assign to first non-Rooftopshop location
        assigned = False
        if item.get('locations'):
            for location in item['locations']:
                if location != "Rooftopshop Magazijn":
                    if location not in location_groups:
                        location_groups[location] = []
                    
                    # Only include id and quantity fields for the API
                    location_groups[location].append({
                        "id": item['id'],
                        "quantity": item['quantity']
                    })
                    assigned = True
                    break
        
        # If no valid location found, add to "unknown" group
        if not assigned:
            if "unknown" not in location_groups:
                location_groups["unknown"] = []
            
            # Only include id and quantity fields for the API
            location_groups["unknown"].append({
                "id": item['id'],
                "quantity": item['quantity']
            })
    
    return location_groups

def split_fulfillment_order(fulfillment_order_id, items_to_split):
    """
    Split the fulfillment order to separate the specified items.
    Returns the ID of the new fulfillment order.
    """
    # Use variables approach for GraphQL
    split_mutation = {
        "query": """
        mutation splitFulfillmentOrder($fulfillmentOrderId: ID!, $lineItems: [FulfillmentOrderLineItemInput!]!) {
          fulfillmentOrderSplit(
            fulfillmentOrderSplits: [
              {
                fulfillmentOrderId: $fulfillmentOrderId,
                fulfillmentOrderLineItems: $lineItems
              }
            ]
          ) {
            fulfillmentOrderSplits {
              fulfillmentOrder {
                id
                status
              }
              remainingFulfillmentOrder {
                id
                status
              }
            }
            userErrors {
              field
              message
            }
          }
        }
        """,
        "variables": {
            "fulfillmentOrderId": fulfillment_order_id,
            "lineItems": items_to_split
        }
    }
    
    logger.info(f"Executing split mutation for {len(items_to_split)} items")
    split_data = send_request_with_retry(split_mutation)
    
    # Check response and handle errors
    if not split_data:
        logger.error("No response from Shopify API for split mutation")
        return None, "No response from Shopify API"
    
    if 'errors' in split_data:
        error_msg = json.dumps(split_data['errors'])
        logger.error(f"GraphQL errors: {error_msg}")
        return None, f"GraphQL errors from Shopify API: {error_msg}"
    
    # Check for user errors
    user_errors = []
    try:
        if ('data' in split_data and 
            split_data['data']['fulfillmentOrderSplit'] and 
            split_data['data']['fulfillmentOrderSplit']['userErrors']):
            user_errors = split_data['data']['fulfillmentOrderSplit']['userErrors']
    except (KeyError, TypeError):
        logger.error("Could not check for user errors in response")
    
    if user_errors:
        error_msg = json.dumps(user_errors)
        logger.error(f"User errors: {error_msg}")
        return None, f"User errors from Shopify API: {error_msg}"
    
    # Extract new fulfillment order details
    try:
        splits = split_data['data']['fulfillmentOrderSplit']['fulfillmentOrderSplits']
        if splits:
            # Extract the new fulfillment order ID
            new_id = None
            if splits[0].get('remainingFulfillmentOrder', {}).get('id'):
                new_id = splits[0]['remainingFulfillmentOrder']['id']
                new_status = splits[0]['remainingFulfillmentOrder'].get('status', 'UNKNOWN')
            elif splits[0].get('fulfillmentOrder', {}).get('id'):
                new_id = splits[0]['fulfillmentOrder']['id']
                new_status = splits[0]['fulfillmentOrder'].get('status', 'UNKNOWN')
            
            if new_id:
                logger.info(f"Successfully split items, new fulfillment order ID: {new_id}")
                return new_id, new_status
            else:
                logger.warning("Split successful but couldn't identify new fulfillment order ID")
                return None, "Split successful but couldn't identify new fulfillment order ID"
        else:
            logger.warning("No splits returned from API")
            return None, "No splits returned from API"
    except (KeyError, IndexError, TypeError) as e:
        logger.error(f"Error parsing split response: {str(e)}")
        return None, f"Error parsing split response: {str(e)}"

def add_tag_to_order(order_id, tag):
    """Add a tag to an order using the GraphQL API."""
    # Check if order_id is None or empty
    if not order_id:
        logger.warning(f"Cannot add tag '{tag}' because order_id is None or empty")
        return False
        
    # Ensure order_id is in the correct format
    if not order_id.startswith('gid://'):
        order_id = f"gid://shopify/Order/{order_id}"
    
    # Create the GraphQL mutation
    mutation = {
        "query": """
        mutation addTags($id: ID!, $tags: [String!]!) {
          tagsAdd(id: $id, tags: $tags) {
            node {
              id
            }
            userErrors {
              message
            }
          }
        }
        """,
        "variables": {
            "id": order_id,
            "tags": [tag]
        }
    }
    
    logger.info(f"Adding tag '{tag}' to order {order_id}")
    result = send_request_with_retry(mutation)
    
    # Check for errors
    if not result:
        logger.error(f"Failed to add tag to order {order_id}")
        return False
    
    if 'errors' in result:
        logger.error(f"GraphQL errors when adding tag: {json.dumps(result['errors'])}")
        return False
    
    # Check for user errors
    user_errors = []
    try:
        if ('data' in result and 
            result['data']['tagsAdd'] and 
            result['data']['tagsAdd']['userErrors']):
            user_errors = result['data']['tagsAdd']['userErrors']
    except (KeyError, TypeError):
        logger.error(f"Could not check for user errors in tagsAdd response")
    
    if user_errors:
        logger.error(f"User errors when adding tag: {json.dumps(user_errors)}")
        return False
    
    logger.info(f"Successfully added tag '{tag}' to order {order_id}")
    return True

def get_location_tag(location):
    """
    Get the appropriate tag for a given location based on the custom rules.
    """
    # Custom tag mapping for specific locations
    if location == "Compri Aluminium":
        return "compriFulfillment"
    elif location == "Redfox EPDM":
        return "redfoxFulfillment"
    else:
        # Default format for other locations
        return f"{location}Fulfillment"

    
def process_fulfillment_according_to_decision_tree(order_id, fulfillment_order_id, categories):
    """
    Process the fulfillment order according to the decision tree logic.
    """
    results = {
        'success': True,
        'splits': [],
        'tags_added': [],
        'error': None
    }
    
    # ENHANCEMENT: Check if we need to add tags regardless of splitting capability
    # For daktrim fulfillment - when all length items are at Rooftopshop
    if categories['summary']['has_length_items'] and len(categories['length_external']) == 0:
        logger.info("Order has length items at Rooftopshop - adding daktrimFulfillment tag")
        if order_id and add_tag_to_order(order_id, "daktrimFulfillment"):
            results['tags_added'].append("daktrimFulfillment")
    
    # For external fulfillment - when all items are at a specific external location
    if len(categories['length_external']) > 0 or len(categories['non_length_external']) > 0:
        # Get unique external locations
        external_locations = set()
        for item in categories['length_external'] + categories['non_length_external']:
            for location in item.get('locations', []):
                if location != "Rooftopshop Magazijn":
                    external_locations.add(location)
        
        # If there's exactly one external location and all items are external
        if len(external_locations) == 1 and (len(categories['length_rooftopshop']) + 
                                           len(categories['non_length_rooftopshop']) +
                                           len(categories['daktrim_koppelstukje_rooftopshop'])) == 0:
            location = next(iter(external_locations))
            tag = get_location_tag(location)
            logger.info(f"All items are from external location {location} - adding {tag} tag")
            if order_id and add_tag_to_order(order_id, tag):
                results['tags_added'].append(tag)

    # First decision: Are there any length items?
    if categories['summary']['has_length_items']:
        logger.info("Order has length items - following 'Yes' branch of decision tree")
        
        # We'll need to track which line items remain in the fulfillment order after each split
        fulfillment_details = None
        
        # Process one category at a time
        # 1. First handle all length items in Rooftopshop along with daktrim_koppelstukje
        if categories['length_rooftopshop']:
            logger.info(f"Processing {len(categories['length_rooftopshop'])} length items at Rooftopshop Magazijn")

            # Combine length items and daktrim_koppelstukje items
            combined_items = []

            # Add length items
            for item in categories['length_rooftopshop']:
                combined_items.append({
                    "id": item["id"],
                    "quantity": item["quantity"]
                })
            
            # Add daktrim koppelstukje items if any - these should be split together with length items
            if categories['daktrim_koppelstukje_rooftopshop']:
                logger.info(f"Including {len(categories['daktrim_koppelstukje_rooftopshop'])} daktrim koppelstukje items with length items")
                for item in categories['daktrim_koppelstukje_rooftopshop']:
                    combined_items.append({
                        "id": item["id"],
                        "quantity": item["quantity"]
                    })
            
            # Log the actual data we're sending to the API
            logger.info(f"Sending items to split API: {json.dumps(combined_items)}")

            new_id, status = split_fulfillment_order(fulfillment_order_id, combined_items)

            if new_id:
                results['splits'].append({
                    'type': 'length_rooftopshop_with_koppelstukje',
                    'fulfillment_order_id': new_id,
                    'status': status,
                    'items': combined_items
                })
                
                # Add daktrimFulfillment tag
                if order_id and add_tag_to_order(order_id, "daktrimFulfillment"):
                    results['tags_added'].append("daktrimFulfillment")
                
                # Add delay after tag operation to allow Shopify to process
                time.sleep(2)
                
                # Get updated fulfillment details after the split
                fulfillment_details = get_fulfillment_order_details(fulfillment_order_id)
                if not fulfillment_details:
                    results['success'] = False
                    results['error'] = "Failed to get updated fulfillment order details after splitting length items at Rooftopshop"
                    return results
            else:
                results['success'] = False
                results['error'] = f"Failed to split length items at Rooftopshop: {status}"
                return results
        
        # 2. Now handle external items - combine length and non-length by location
        if not categories['summary']['all_items_at_rooftopshop']:
            logger.info("Processing external items by location (both length and non-length together)")
            
            # If we don't have updated fulfillment details yet, get them now
            if not fulfillment_details:
                fulfillment_details = get_fulfillment_order_details(fulfillment_order_id)
                if not fulfillment_details:
                    results['success'] = False
                    results['error'] = "Failed to get fulfillment order details before splitting external items"
                    return results
            
            # Create a map of remaining line items to check against
            remaining_line_items = {item['fulfillment_order_line_item_id']: item for item in fulfillment_details['line_items']}
            logger.info(f"Remaining items before external processing: {len(remaining_line_items)}")
            
            # Group by location - combining both length and non-length external items
            location_groups = {}
            
            # Process length external items
            for item in categories['length_external']:
                if item['id'] in remaining_line_items:
                    # Get the locations from the remaining item
                    locations = remaining_line_items[item['id']]['available_locations']
                    if not isinstance(locations, list):
                        locations = [locations] if locations else []
                    
                    # Assign to first non-Rooftopshop location
                    assigned = False
                    for location in locations:
                        if location != "Rooftopshop Magazijn":
                            if location not in location_groups:
                                location_groups[location] = []
                            
                            location_groups[location].append({
                                "id": item['id'],
                                "quantity": item['quantity']
                            })
                            assigned = True
                            break
                    
                    # If no valid location found, add to "unknown" group
                    if not assigned and locations:
                        if "unknown" not in location_groups:
                            location_groups["unknown"] = []
                        
                        location_groups["unknown"].append({
                            "id": item['id'],
                            "quantity": item['quantity']
                        })
            
            # Process non-length external items and add to the same location groups
            for item in categories['non_length_external']:
                if item['id'] in remaining_line_items:
                    # Get the locations from the remaining item
                    locations = remaining_line_items[item['id']]['available_locations']
                    if not isinstance(locations, list):
                        locations = [locations] if locations else []
                    
                    # Assign to first non-Rooftopshop location
                    assigned = False
                    for location in locations:
                        if location != "Rooftopshop Magazijn":
                            if location not in location_groups:
                                location_groups[location] = []
                            
                            location_groups[location].append({
                                "id": item['id'],
                                "quantity": item['quantity']
                            })
                            assigned = True
                            break
                    
                    # If no valid location found, add to "unknown" group
                    if not assigned and locations:
                        if "unknown" not in location_groups:
                            location_groups["unknown"] = []
                        
                        location_groups["unknown"].append({
                            "id": item['id'],
                            "quantity": item['quantity']
                        })
            
            # Log the location groups before processing
            logger.info(f"Location groups created: {json.dumps({loc: len(items) for loc, items in location_groups.items()})}")
            
            # Process each location group
            for location, items in location_groups.items():
                if not items:
                    continue
                
                logger.info(f"Splitting {len(items)} items (both length and non-length) for external location: {location}")
                
                # Log the actual data we're sending to the API
                logger.info(f"Sending items to split API: {json.dumps(items)}")
                
                new_id, status = split_fulfillment_order(fulfillment_order_id, items)
                
                # Add substantial delay after split operation to ensure Shopify has time to process
                time.sleep(3)
                
                if new_id:
                    results['splits'].append({
                        'type': f'external_{location}',
                        'fulfillment_order_id': new_id,
                        'status': status,
                        'items': items,
                        'location': location
                    })
                    
                    # Add location tag with the custom format
                    tag = get_location_tag(location)
                    if order_id and add_tag_to_order(order_id, tag):
                        results['tags_added'].append(tag)
                    
                    # Add delay after tag operation
                    time.sleep(1)
                    
                    # Update fulfillment details after each split
                    logger.info(f"Getting updated fulfillment details after splitting items at {location}")
                    fulfillment_details = get_fulfillment_order_details(fulfillment_order_id)
                    if not fulfillment_details:
                        results['success'] = False
                        results['error'] = f"Failed to get updated fulfillment order details after splitting items at {location}"
                        return results
                    
                    # Update remaining line items map
                    remaining_line_items = {item['fulfillment_order_line_item_id']: item for item in fulfillment_details['line_items']}
                    logger.info(f"Remaining items after {location} split: {len(remaining_line_items)}")
                else:
                    logger.error(f"Failed to split items for {location}: {status}")
                    results['success'] = False
                    results['error'] = f"Failed to split items for {location}: {status}"
                    return results
    else:
        logger.info("Order has no length items - following 'No' branch of decision tree")

        # When there are no length items in the order, treat daktrim_koppelstukje items as regular non-length items
        if categories['daktrim_koppelstukje_rooftopshop']:
            logger.info(f"Adding {len(categories['daktrim_koppelstukje_rooftopshop'])} daktrim koppelstukje items to non-length items category")
            for item in categories['daktrim_koppelstukje_rooftopshop']:
                categories['non_length_rooftopshop'].append(item)
        
        # Decision: Are all non-length products available at Rooftopshop Magazijn?
        if categories['summary']['all_non_length_at_rooftopshop']:
            logger.info("All non-length items are at Rooftopshop Magazijn - ready for picking")
            # Nothing to do, everything is at Rooftopshop and ready for picking
        else:
            logger.info("Not all non-length items are at Rooftopshop - splitting by location")
            
            # Get updated fulfillment details
            fulfillment_details = get_fulfillment_order_details(fulfillment_order_id)
            if not fulfillment_details:
                results['success'] = False
                results['error'] = "Failed to get fulfillment order details before splitting external non-length items"
                return results
            
            # Create a map of remaining line items
            remaining_line_items = {item['fulfillment_order_line_item_id']: item for item in fulfillment_details['line_items']}
            logger.info(f"Remaining items before non-length external processing: {len(remaining_line_items)}")
            
            # Group by location
            location_groups = {}
            
            # Only process items that are still in the current fulfillment order
            for item in categories['non_length_external']:
                if item['id'] in remaining_line_items:
                    # Get the locations from the remaining item - ensure it's a list
                    locations = remaining_line_items[item['id']]['available_locations']
                    if not isinstance(locations, list):
                        # Convert to list if it's not already
                        locations = [locations] if locations else []
                    
                    # Assign to first non-Rooftopshop location
                    assigned = False
                    for location in locations:
                        if location != "Rooftopshop Magazijn":
                            if location not in location_groups:
                                location_groups[location] = []
                            
                            location_groups[location].append({
                                "id": item['id'],
                                "quantity": item['quantity']
                            })
                            assigned = True
                            break
                    
                    # If no valid location found, add to "unknown" group
                    if not assigned and locations:
                        if "unknown" not in location_groups:
                            location_groups["unknown"] = []
                        
                        location_groups["unknown"].append({
                            "id": item['id'],
                            "quantity": item['quantity']
                        })
            
            # Log the location groups before processing
            logger.info(f"Non-length location groups created: {json.dumps({loc: len(items) for loc, items in location_groups.items()})}")
            
            # Process each location group
            for location, items in location_groups.items():
                if not items:
                    continue
                
                logger.info(f"Splitting {len(items)} non-length items for external location: {location}")
                
                # Log the items we're about to split
                logger.info(f"Sending items to split API: {json.dumps(items)}")
                
                new_id, status = split_fulfillment_order(fulfillment_order_id, items)
                
                # Add substantial delay after split operation
                time.sleep(3)
                
                if new_id:
                    results['splits'].append({
                        'type': f'external_{location}',
                        'fulfillment_order_id': new_id,
                        'status': status,
                        'items': items,
                        'location': location
                    })
                    
                    # Add location tag with custom naming pattern
                    tag = get_location_tag(location)
                    if order_id and add_tag_to_order(order_id, tag):
                        results['tags_added'].append(tag)
                    
                    # Add delay after tag operation
                    time.sleep(1)
                    
                    # Update fulfillment details after each split
                    logger.info(f"Getting updated fulfillment details after splitting non-length items at {location}")
                    fulfillment_details = get_fulfillment_order_details(fulfillment_order_id)
                    if not fulfillment_details:
                        results['success'] = False
                        results['error'] = f"Failed to get updated fulfillment order details after splitting non-length items at {location}"
                        return results
                    
                    # Update remaining line items map
                    remaining_line_items = {item['fulfillment_order_line_item_id']: item for item in fulfillment_details['line_items']}
                    logger.info(f"Remaining items after {location} split: {len(remaining_line_items)}")
                else:
                    logger.error(f"Failed to split non-length items for {location}: {status}")
                    results['success'] = False
                    results['error'] = f"Failed to split non-length items for {location}: {status}"
                    break
    
    return results

def lambda_handler(event, context):
    """
    AWS Lambda handler function for processing fulfillment orders according to the decision tree.
    """
    try:
        logger.info("Processing fulfillment order according to decision tree...")
        
        # Parse the body of the request
        body = None
        if isinstance(event, dict):
            if 'body' in event:
                # API Gateway format
                if isinstance(event['body'], str):
                    body = json.loads(event['body'])
                else:
                    body = event['body']
            else:
                # Direct invocation
                body = event
        
        if not body:
            logger.error("No request body found")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'No request body found'})
            }
        
        # Extract OrderId and FulfillmentOrderId from the request
        order_id = None
        fulfillment_order_id = None
        
        if 'OrderId' in body:
            order_id = body['OrderId']
        elif 'orderId' in body:
            order_id = body['orderId']
        
        if 'FulfillmentOrderId' in body:
            fulfillment_order_id = body['FulfillmentOrderId']
        elif 'fulfillmentOrderId' in body:
            fulfillment_order_id = body['fulfillmentOrderId']
        elif 'id' in body:
            fulfillment_order_id = body['id']
        
        if not fulfillment_order_id:
            logger.error("Missing FulfillmentOrderId in the request")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'FulfillmentOrderId is required'})
            }
        
        # Log both IDs
        logger.info(f"Processing order: {order_id if order_id else 'None'}, fulfillment order: {fulfillment_order_id}")
        
        # Ensure the fulfillment order ID is in the correct format
        if not fulfillment_order_id.startswith('gid://'):
            fulfillment_order_id = f"gid://shopify/FulfillmentOrder/{fulfillment_order_id}"
        
        # 1. Get fulfillment order details with line items and metafields
        order_details = get_fulfillment_order_details(fulfillment_order_id)
        
        if not order_details:
            logger.error(f"Failed to get details for fulfillment order {fulfillment_order_id}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': f'Failed to get details for fulfillment order {fulfillment_order_id}'
                })
            }
        
        # Check if the fulfillment order has any line items
        if not order_details['line_items']:
            logger.info(f"Fulfillment order {fulfillment_order_id} has no line items to process")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'success': True,
                    'message': f"Fulfillment order {fulfillment_order_id} has no line items to process",
                    'fulfillment_order_id': fulfillment_order_id,
                    'status': order_details['status'],
                    'line_items_count': 0
                })
            }
        
        # 2. Categorize items according to the decision tree
        categories = categorize_items(order_details)
        
        # 3. Process fulfillment according to decision tree logic
        results = process_fulfillment_according_to_decision_tree(
            order_id, 
            fulfillment_order_id, 
            categories
        )
        
        # 4. Prepare response
        response_data = {
            'success': results['success'],
            'fulfillment_order_id': fulfillment_order_id,
            'order_id': order_id,
            'splits': results['splits'],
            'tags_added': results['tags_added'],
            'item_categories': {
                'length_items_count': len(categories['length_rooftopshop']) + len(categories['length_external']),
                'non_length_items_count': len(categories['non_length_rooftopshop']) + len(categories['non_length_external']),
                'items_at_rooftopshop': len(categories['length_rooftopshop']) + len(categories['non_length_rooftopshop']),
                'items_at_external': len(categories['length_external']) + len(categories['non_length_external'])
            }
        }
        
        if results['error']:
            response_data['error'] = results['error']
        
        # Format the response based on the invocation type
        if 'httpMethod' in event:
            # API Gateway response
            return {
                'statusCode': 200 if results['success'] else 500,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps(response_data)
            }
        else:
            # Direct invocation response
            return response_data
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Format error response based on invocation type
        if 'httpMethod' in event:
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({
                    'success': False,
                    'error': f'Unexpected error: {str(e)}'
                })
            }
        else:
            return {
                'success': False,
                'error': f'Unexpected error: {str(e)}',
                'traceback': traceback.format_exc()
            }
