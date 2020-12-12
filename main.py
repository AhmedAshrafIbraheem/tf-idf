from compute_tf_idf import tf_idf, query


def app():
    print("Welcome to Our App")
    while True:
        user_input = input(">>> ").split()
        if len(user_input) < 1:
            continue
        keyword = user_input[0].lower()
        if keyword.__eq__('tf_idf'):
            tf_idf("project2_demo.txt")
        elif keyword.__eq__('query'):
            if len(user_input) > 1:
                query(" ".join(user_input[1:]))
            else:
                print('Missing Term Name')
        elif keyword.__eq__('exit'):
            print('Good Bye, I hope you enjoyed using my App !!!')
            break
        else:
            print('Unexpected Input')


if __name__ == '__main__':
    app()
