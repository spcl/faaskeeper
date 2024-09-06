import functions_framework

# Register a dummy function for meeting the requirement of deploying a function in functions section in serverless template
@functions_framework.http
def dummy_http_function(request):
  # Your code here

  # Return an HTTP response
  return 'OK'