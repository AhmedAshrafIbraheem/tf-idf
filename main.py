from compute_tf_idf import tf_idf


def app():
    print("Welcome to Our App")
    while True:
        query = input(">>> ").split()
        keyword = query[0].lower()
        if keyword.__eq__('tf_idf'):
            tf_idf("/home/ahmed/Desktop/Tf-Idf/project2_demo.txt")
        elif keyword.__eq__('query'):
            # Execute Query
            pass
        elif keyword.__eq__('exit'):
            print('Good Bye, I hope you enjoyed using my App !!!')
            break
        else:
            print('Unexpected Input')


if __name__ == '__main__':
    app()
