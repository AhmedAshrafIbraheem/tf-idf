from compute_tf_idf import tf_idf, query


def app():
    print("Welcome to Our App")
    while True:
        user_input = input(">>> ").split()
        keyword = user_input[0].lower()
        if keyword.__eq__('tf_idf'):
            tf_idf("/home/ahmed/Desktop/Tf-Idf/project2_test.txt")
        elif keyword.__eq__('query'):
            query(" ".join(user_input[1:]))
        elif keyword.__eq__('exit'):
            print('Good Bye, I hope you enjoyed using my App !!!')
            break
        else:
            print('Unexpected Input')


if __name__ == '__main__':
    app()
